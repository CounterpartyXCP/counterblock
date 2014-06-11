import os
import json
import re
import time
import datetime
import base64
import decimal
import operator
import logging
import copy
import uuid

from logging import handlers as logging_handlers
from gevent import pywsgi
import cherrypy
from cherrypy.process import plugins
from jsonrpc import JSONRPCResponseManager, dispatcher
import pymongo
from bson import json_util
from bson.son import SON

from . import (config, siofeeds, util, util_trading, betting)

PREFERENCES_MAX_LENGTH = 100000 #in bytes, as expressed in JSON
D = decimal.Decimal


def serve_api(mongo_db, redis_client):
    # Preferneces are just JSON objects... since we don't force a specific form to the wallet on
    # the server side, this makes it easier for 3rd party wallets (i.e. not Counterwallet) to fully be able to
    # use counterblockd to not only pull useful data, but also load and store their own preferences, containing
    # whatever data they need
    
    DEFAULT_COUNTERPARTYD_API_CACHE_PERIOD = 60 #in seconds
    tx_logger = logging.getLogger("transaction_log") #get transaction logger
    
    @dispatcher.add_method
    def is_ready():
        """this method used by the client to check if the server is alive, caught up, and ready to accept requests.
        If the server is NOT caught up, a 525 error will be returned actually before hitting this point. Thus,
        if we actually return data from this function, it should always be true. (may change this behaviour later)"""
        
        insightInfo = util.call_insight_api('/api/status?q=getInfo', abort_on_error=True)
        return {
            'caught_up': util.is_caught_up_well_enough_for_government_work(),
            'last_message_index': config.LAST_MESSAGE_INDEX,
            'block_height': insightInfo['info']['blocks'], 
            'testnet': config.TESTNET 
        }
    
    @dispatcher.add_method
    def get_reflected_host_info():
        """Allows the requesting host to get some info about itself, such as its IP. Used for troubleshooting."""
        return {
            'ip': cherrypy.request.headers.get('X-Real-Ip', cherrypy.request.headers.get('Remote-Addr', '')),
            'cookie': cherrypy.request.headers.get('Cookie', '')
        }
    
    @dispatcher.add_method
    def get_messagefeed_messages_by_index(message_indexes): #yeah, dumb name :)
        messages = util.call_jsonrpc_api("get_messages_by_index", [message_indexes,], abort_on_error=True)['result']
        events = []
        for m in messages:
            events.append(util.decorate_message_for_feed(m))
        return events

    @dispatcher.add_method
    def get_btc_block_height():
        data = util.call_insight_api('/api/status?q=getInfo', abort_on_error=True)
        return data['info']['blocks']

    @dispatcher.add_method
    def get_btc_address_info(addresses, with_uxtos=True, with_last_txn_hashes=4, with_block_height=False):
        if not isinstance(addresses, list):
            raise Exception("addresses must be a list of addresses, even if it just contains one address")
        results = []
        if with_block_height:
            block_height_response = util.call_insight_api('/api/status?q=getInfo', abort_on_error=True)
            block_height = block_height_response['info']['blocks'] if block_height_response else None
        for address in addresses:
            info = util.call_insight_api('/api/addr/' + address + '/', abort_on_error=True)
            txns = info['transactions']
            del info['transactions']

            result = {}
            result['addr'] = address
            result['info'] = info
            if with_block_height: result['block_height'] = block_height
            #^ yeah, hacky...it will be the same block height for each address (we do this to avoid an extra API call to get_btc_block_height)
            if with_uxtos:
                result['uxtos'] = util.call_insight_api('/api/addr/' + address + '/utxo/', abort_on_error=True)
            if with_last_txn_hashes:
                #with last_txns, only show CONFIRMED txns (so skip the first info['unconfirmedTxApperances'] # of txns, if not 0
                result['last_txns'] = txns[info['unconfirmedTxApperances']:with_last_txn_hashes+info['unconfirmedTxApperances']]
            results.append(result)
        return results

    @dispatcher.add_method
    def get_btc_txns_status(txn_hashes):
        if not isinstance(txn_hashes, list):
            raise Exception("txn_hashes must be a list of txn hashes, even if it just contains one hash")
        results = []
        for tx_hash in txn_hashes:
            tx_info = util.call_insight_api('/api/tx/' + tx_hash + '/', abort_on_error=False)
            if tx_info:
                assert tx_info['txid'] == tx_hash
                results.append({
                    'tx_hash': tx_info['txid'],
                    'blockhash': tx_info.get('blockhash', None), #not provided if not confirmed on network
                    'confirmations': tx_info.get('confirmations', 0), #not provided if not confirmed on network
                    'blocktime': tx_info.get('time', None),
                })
        return results

    @dispatcher.add_method
    def get_normalized_balances(addresses):
        """
        This call augments counterpartyd's get_balances with a normalized_quantity field. It also will include any owned
        assets for an address, even if their balance is zero. 
        NOTE: Does not retrieve BTC balance. Use get_btc_address_info for that.
        """
        if not isinstance(addresses, list):
            raise Exception("addresses must be a list of addresses, even if it just contains one address")
        if not len(addresses):
            raise Exception("Invalid address list supplied")
        
        filters = []
        for address in addresses:
            filters.append({'field': 'address', 'op': '==', 'value': address})
        
        mappings = {}
        result = util.call_jsonrpc_api("get_balances",
            {'filters': filters, 'filterop': 'or'}, abort_on_error=True)['result']

        isowner = {}
        owned_assets = mongo_db.tracked_assets.find( { '$or': [{'owner': a } for a in addresses] }, { '_history': 0, '_id': 0 } )
        for o in owned_assets:
          isowner[o['owner'] + o['asset']] = o

        data = []
        for d in result:
            if not d['quantity'] and ((d['address'] + d['asset']) not in isowner):
                continue #don't include balances with a zero asset value
            asset_info = mongo_db.tracked_assets.find_one({'asset': d['asset']})
            d['normalized_quantity'] = util.normalize_quantity(d['quantity'], asset_info['divisible'])
            d['owner'] = (d['address'] + d['asset']) in isowner
            mappings[d['address'] + d['asset']] = d
            data.append(d)
        
        #include any owned assets for each address, even if their balance is zero
        for key in isowner:
            if key not in mappings:
                o = isowner[key]
                data.append({
                    'address': o['owner'],
                    'asset': o['asset'],
                    'quantity': 0,
                    'normalized_quantity': 0,
                    'owner': True,
                })

        return data

    def _get_address_history(address, start_block=None, end_block=None):
        address_dict = {}
        
        address_dict['balances'] = util.call_jsonrpc_api("get_balances",
            { 'filters': [{'field': 'address', 'op': '==', 'value': address},],
            }, abort_on_error=True)['result']
        
        address_dict['debits'] = util.call_jsonrpc_api("get_debits",
            { 'filters': [{'field': 'address', 'op': '==', 'value': address},],
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
        
        address_dict['credits'] = util.call_jsonrpc_api("get_credits",
            { 'filters': [{'field': 'address', 'op': '==', 'value': address},],
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
    
        address_dict['burns'] = util.call_jsonrpc_api("get_burns",
            { 'filters': [{'field': 'source', 'op': '==', 'value': address},],
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
    
        address_dict['sends'] = util.call_jsonrpc_api("get_sends",
            { 'filters': [{'field': 'source', 'op': '==', 'value': address}, {'field': 'destination', 'op': '==', 'value': address}],
              'filterop': 'or',
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
        #^ with filterop == 'or', we get all sends where this address was the source OR destination 
        
        address_dict['orders'] = util.call_jsonrpc_api("get_orders",
            { 'filters': [{'field': 'source', 'op': '==', 'value': address},],
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']

        address_dict['order_matches'] = util.call_jsonrpc_api("get_order_matches",
            { 'filters': [{'field': 'tx0_address', 'op': '==', 'value': address}, {'field': 'tx1_address', 'op': '==', 'value': address},],
              'filterop': 'or',
              'order_by': 'tx0_block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
        
        address_dict['btcpays'] = util.call_jsonrpc_api("get_btcpays",
            { 'filters': [{'field': 'source', 'op': '==', 'value': address}, {'field': 'destination', 'op': '==', 'value': address}],
              'filterop': 'or',
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
        
        address_dict['issuances'] = util.call_jsonrpc_api("get_issuances",
            { 'filters': [{'field': 'issuer', 'op': '==', 'value': address}, {'field': 'source', 'op': '==', 'value': address}],
              'filterop': 'or',
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
        
        address_dict['broadcasts'] = util.call_jsonrpc_api("get_broadcasts",
            { 'filters': [{'field': 'source', 'op': '==', 'value': address},],
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']

        address_dict['bets'] = util.call_jsonrpc_api("get_bets",
            { 'filters': [{'field': 'source', 'op': '==', 'value': address},],
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
        
        address_dict['bet_matches'] = util.call_jsonrpc_api("get_bet_matches",
            { 'filters': [{'field': 'tx0_address', 'op': '==', 'value': address}, {'field': 'tx1_address', 'op': '==', 'value': address},],
              'filterop': 'or',
              'order_by': 'tx0_block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
        
        address_dict['dividends'] = util.call_jsonrpc_api("get_dividends",
            { 'filters': [{'field': 'source', 'op': '==', 'value': address},],
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
        
        address_dict['cancels'] = util.call_jsonrpc_api("get_cancels",
            { 'filters': [{'field': 'source', 'op': '==', 'value': address},],
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
    
        address_dict['callbacks'] = util.call_jsonrpc_api("get_callbacks",
            { 'filters': [{'field': 'source', 'op': '==', 'value': address},],
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
    
        address_dict['bet_expirations'] = util.call_jsonrpc_api("get_bet_expirations",
            { 'filters': [{'field': 'source', 'op': '==', 'value': address},],
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
    
        address_dict['order_expirations'] = util.call_jsonrpc_api("get_order_expirations",
            { 'filters': [{'field': 'source', 'op': '==', 'value': address},],
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
    
        address_dict['bet_match_expirations'] = util.call_jsonrpc_api("get_bet_match_expirations",
            { 'filters': [{'field': 'tx0_address', 'op': '==', 'value': address}, {'field': 'tx1_address', 'op': '==', 'value': address},],
              'filterop': 'or',
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
    
        address_dict['order_match_expirations'] = util.call_jsonrpc_api("get_order_match_expirations",
            { 'filters': [{'field': 'tx0_address', 'op': '==', 'value': address}, {'field': 'tx1_address', 'op': '==', 'value': address},],
              'filterop': 'or',
              'order_by': 'block_index',
              'order_dir': 'asc',
              'start_block': start_block,
              'end_block': end_block,
            }, abort_on_error=True)['result']
    
        return address_dict

    @dispatcher.add_method
    def get_last_n_messages(count=100):
        if count > 1000:
            raise Exception("The count is too damn high")
        message_indexes = range(max(config.LAST_MESSAGE_INDEX - count, 0) + 1, config.LAST_MESSAGE_INDEX+1)
        messages = util.call_jsonrpc_api("get_messages_by_index",
            { 'message_indexes': message_indexes }, abort_on_error=True)['result']
        for i in xrange(len(messages)):
            messages[i] = util.decorate_message_for_feed(messages[i])
        return messages

    @dispatcher.add_method
    def get_raw_transactions(address, start_ts=None, end_ts=None, limit=500):
        """Gets raw transactions for a particular address
        
        @param address: A single address string
        @param start_ts: The starting date & time. Should be a unix epoch object. If passed as None, defaults to 30 days before the end_date
        @param end_ts: The ending date & time. Should be a unix epoch object. If passed as None, defaults to the current date & time
        @param limit: the maximum number of transactions to return; defaults to ten thousand
        @return: Returns the data, ordered from newest txn to oldest. If any limit is applied, it will cut back from the oldest results
        """
        def get_asset_cached(asset, asset_cache):
            if asset in asset_cache:
                return asset_cache[asset]
            asset_data = mongo_db.tracked_assets.find_one({'asset': asset})
            asset_cache[asset] = asset_data
            return asset_data
        
        asset_cache = {} #ghetto cache to speed asset lookups within the scope of a function call
        
        if not end_ts: #default to current datetime
            end_ts = time.mktime(datetime.datetime.utcnow().timetuple())
        if not start_ts: #default to 30 days before the end date
            start_ts = end_ts - (30 * 24 * 60 * 60) 
        start_block_index, end_block_index = util.get_block_indexes_for_dates(
            datetime.datetime.utcfromtimestamp(start_ts), datetime.datetime.utcfromtimestamp(end_ts))
        
        #make API call to counterpartyd to get all of the data for the specified address
        txns = []
        d = _get_address_history(address, start_block=start_block_index, end_block=end_block_index)
        #mash it all together
        for category, entries in d.iteritems():
            if category in ['balances',]:
                continue
            for e in entries:
                e['_category'] = category
                e = util.decorate_message(e, for_txn_history=True) #DRY
            txns += entries
        txns = util.multikeysort(txns, ['-_block_time', '-_tx_index'])
        txns = txns[0:limit] #TODO: we can trunk before sorting. check if we can use the messages table and use sql order and limit
        #^ won't be a perfect sort since we don't have tx_indexes for cancellations, but better than nothing
        #txns.sort(key=operator.itemgetter('block_index'))
        return txns 

    @dispatcher.add_method
    def get_base_quote_asset(asset1, asset2):
        """Given two arbitrary assets, returns the base asset and the quote asset.
        """
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        base_asset_info = mongo_db.tracked_assets.find_one({'asset': base_asset})
        quote_asset_info = mongo_db.tracked_assets.find_one({'asset': quote_asset})
        pair_name = "%s/%s" % (base_asset, quote_asset)

        if not base_asset_info or not quote_asset_info:
            raise Exception("Invalid asset(s)")

        return {
            'base_asset': base_asset,
            'quote_asset': quote_asset,
            'pair_name': pair_name
        }

    @dispatcher.add_method
    def get_market_price_summary(asset1, asset2, with_last_trades=0):
        result = util_trading.get_market_price_summary(asset1, asset2, with_last_trades)
        return result if result is not None else False
        #^ due to current bug in our jsonrpc stack, just return False if None is returned

    @dispatcher.add_method
    def get_market_cap_history(start_ts=None, end_ts=None):
        if not end_ts: #default to current datetime
            end_ts = time.mktime(datetime.datetime.utcnow().timetuple())
        if not start_ts: #default to 30 days before the end date
            start_ts = end_ts - (30 * 24 * 60 * 60) 
        
        data = {}
        results = {}
        #^ format is result[market_cap_as][asset] = [[block_time, market_cap], [block_time2, market_cap2], ...] 
        for market_cap_as in ('XCP', 'BTC'):
            caps = mongo_db.asset_marketcap_history.aggregate([
                {"$match": {
                    "market_cap_as": market_cap_as,
                    "block_time": {
                        "$gte": datetime.datetime.utcfromtimestamp(start_ts),
                        "$lte": datetime.datetime.utcfromtimestamp(end_ts)
                    }
                }},
                {"$project": {
                    "year":  {"$year": "$block_time"},
                    "month": {"$month": "$block_time"},
                    "day":   {"$dayOfMonth": "$block_time"},
                    "hour":  {"$hour": "$block_time"},
                    "asset": 1,
                    "market_cap": 1,
                }},
                {"$sort": {"block_time": pymongo.ASCENDING}},
                {"$group": {
                    "_id":   {"asset": "$asset", "year": "$year", "month": "$month", "day": "$day", "hour": "$hour"},
                    "market_cap": {"$avg": "$market_cap"}, #use the average marketcap during the interval
                }},
            ])
            caps = [] if not caps['ok'] else caps['result']
            data[market_cap_as] = {}
            for e in caps:
                interval_time = int(time.mktime(datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day'], e['_id']['hour']).timetuple()) * 1000)
                data[market_cap_as].setdefault(e['_id']['asset'], [])
                data[market_cap_as][e['_id']['asset']].append([interval_time, e['market_cap']])
            results[market_cap_as] = []
            for asset in data[market_cap_as]:
                #for z in data[market_cap_as][asset]: assert z[0] and z[0] > 0 and z[1] and z[1] >= 0
                results[market_cap_as].append({'name': asset, 
                    'data': sorted(data[market_cap_as][asset], key=operator.itemgetter(0))})
        return results 

    @dispatcher.add_method
    def get_market_info(assets):
        assets_market_info = list(mongo_db.asset_market_info.find({'asset': {'$in': assets}}, {'_id': 0}))
        extended_asset_info = mongo_db.asset_extended_info.find({'asset': {'$in': assets}})
        extended_asset_info_dict = {}
        for e in extended_asset_info:
            if not e.get('disabled', False): #skip assets marked disabled
                extended_asset_info_dict[e['asset']] = e
        for a in assets_market_info:
            if a['asset'] in extended_asset_info_dict and extended_asset_info_dict[a['asset']].get('processed', False):
                extended_info = extended_asset_info_dict[a['asset']]
                a['extended_image'] = bool(extended_info['image'])
                a['extended_description'] = extended_info['description']
                a['extended_website'] = extended_info['website']
                a['extended_pgpsig'] = extended_info['pgpsig']
            else:
                a['extended_image'] = a['extended_description'] = a['extended_website'] = a['extended_pgpsig'] = ''
        return assets_market_info

    @dispatcher.add_method
    def get_market_info_leaderboard(limit=100):
        """returns market leaderboard data for both the XCP and BTC markets"""
        #do two queries because we limit by our sorted results, and we might miss an asset with a high BTC trading value
        # but with little or no XCP trading activity, for instance if we just did one query
        assets_market_info_xcp = list(mongo_db.asset_market_info.find({}, {'_id': 0}).sort('market_cap_in_xcp', pymongo.DESCENDING).limit(limit))
        assets_market_info_btc = list(mongo_db.asset_market_info.find({}, {'_id': 0}).sort('market_cap_in_btc', pymongo.DESCENDING).limit(limit))
        assets_market_info = {
            'xcp': [a for a in assets_market_info_xcp if a['price_in_xcp']],
            'btc': [a for a in assets_market_info_btc if a['price_in_btc']]
        }
        #throw on extended info, if it exists for a given asset
        assets = list(set([a['asset'] for a in assets_market_info['xcp']] + [a['asset'] for a in assets_market_info['btc']]))
        extended_asset_info = mongo_db.asset_extended_info.find({'asset': {'$in': assets}})
        extended_asset_info_dict = {}
        for e in extended_asset_info:
            if not e.get('disabled', False): #skip assets marked disabled
                extended_asset_info_dict[e['asset']] = e
        for r in (assets_market_info['xcp'], assets_market_info['btc']):
            for a in r:
                if a['asset'] in extended_asset_info_dict:
                    extended_info = extended_asset_info_dict[a['asset']]
                    if 'extended_image' not in a or 'extended_description' not in a or 'extended_website' not in a:
                        continue #asset has been recognized as having a JSON file description, but has not been successfully processed yet
                    a['extended_image'] = bool(extended_info['image'])
                    a['extended_description'] = extended_info['description']
                    a['extended_website'] = extended_info['website']
                else:
                    a['extended_image'] = a['extended_description'] = a['extended_website'] = ''
        return assets_market_info

    @dispatcher.add_method
    def get_market_price_history(asset1, asset2, start_ts=None, end_ts=None, as_dict=False):
        """Return block-by-block aggregated market history data for the specified asset pair, within the specified date range.
        @returns List of lists (or list of dicts, if as_dict is specified).
            * If as_dict is False, each embedded list has 8 elements [block time (epoch in MS), open, high, low, close, volume, # trades in block, block index]
            * If as_dict is True, each dict in the list has the keys: block_time (epoch in MS), block_index, open, high, low, close, vol, count
            
        Aggregate on an an hourly basis 
        """
        if not end_ts: #default to current datetime
            end_ts = time.mktime(datetime.datetime.utcnow().timetuple())
        if not start_ts: #default to 180 days before the end date
            start_ts = end_ts - (180 * 24 * 60 * 60) 
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        
        #get ticks -- open, high, low, close, volume
        result = mongo_db.trades.aggregate([
            {"$match": {
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "block_time": {
                    "$gte": datetime.datetime.utcfromtimestamp(start_ts),
                    "$lte": datetime.datetime.utcfromtimestamp(end_ts)
                }
            }},
            {"$project": {
                "year":  {"$year": "$block_time"},
                "month": {"$month": "$block_time"},
                "day":   {"$dayOfMonth": "$block_time"},
                "hour":  {"$hour": "$block_time"},
                "block_index": 1,
                "unit_price": 1,
                "base_quantity_normalized": 1 #to derive volume
            }},
            {"$group": {
                "_id":   {"year": "$year", "month": "$month", "day": "$day", "hour": "$hour"},
                "open":  {"$first": "$unit_price"},
                "high":  {"$max": "$unit_price"},
                "low":   {"$min": "$unit_price"},
                "close": {"$last": "$unit_price"},
                "vol":   {"$sum": "$base_quantity_normalized"},
                "count": {"$sum": 1},
            }},
            {"$sort": SON([("_id.year", pymongo.ASCENDING), ("_id.month", pymongo.ASCENDING), ("_id.day", pymongo.ASCENDING), ("_id.hour", pymongo.ASCENDING)])},
        ])
        if not result['ok'] or not len(result['result']):
            return False
        result = result['result']
        
        midline = [((r['high'] + r['low']) / 2.0) for r in result]
        if as_dict:
            for i in xrange(len(result)):
                result[i]['interval_time'] = int(time.mktime(datetime.datetime(
                    result[i]['_id']['year'], result[i]['_id']['month'], result[i]['_id']['day'], result[i]['_id']['hour']).timetuple()) * 1000)
                result[i]['midline'] = midline[i]
                del result[i]['_id']
            return result
        else:
            list_result = []
            for i in xrange(len(result)):
                list_result.append([
                    int(time.mktime(datetime.datetime(
                        result[i]['_id']['year'], result[i]['_id']['month'], result[i]['_id']['day'], result[i]['_id']['hour']).timetuple()) * 1000),
                    result[i]['open'], result[i]['high'], result[i]['low'], result[i]['close'], result[i]['vol'],
                    result[i]['count'], midline[i]
                ])
            return list_result
    
    @dispatcher.add_method
    def get_trade_history(asset1=None, asset2=None, start_ts=None, end_ts=None, limit=50):
        """
        Gets last N of trades within a specific date range (normally, for a specified asset pair, but this can
        be left blank to get any/all trades).
        """
        assert (asset1 and asset2) or (not asset1 and not asset2) #cannot have one asset, but not the other

        if limit > 500:
            raise Exception("Requesting history of too many trades")

        if not end_ts: #default to current datetime
            end_ts = time.mktime(datetime.datetime.utcnow().timetuple())
        if not start_ts: #default to 30 days before the end date
            start_ts = end_ts - (30 * 24 * 60 * 60) 

        filters = {
            "block_time": {
                "$gte": datetime.datetime.utcfromtimestamp(start_ts),
                "$lte": datetime.datetime.utcfromtimestamp(end_ts)
            }
        }            
        if asset1 and asset2:
            base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
            filters["base_asset"] = base_asset
            filters["quote_asset"] = quote_asset

        last_trades = mongo_db.trades.find(filters, {'_id': 0}).sort("block_time", pymongo.DESCENDING).limit(limit)
        if not last_trades.count():
            return False #no suitable trade data to form a market price
        last_trades = list(last_trades)
        return last_trades 

    def _get_order_book(base_asset, quote_asset,
    bid_book_min_pct_fee_provided=None, bid_book_min_pct_fee_required=None, bid_book_max_pct_fee_required=None,
    ask_book_min_pct_fee_provided=None, ask_book_min_pct_fee_required=None, ask_book_max_pct_fee_required=None):
        """Gets the current order book for a specified asset pair
        
        @param: normalized_fee_required: Only specify if buying BTC. If specified, the order book will be pruned down to only
         show orders at and above this fee_required
        @param: normalized_fee_provided: Only specify if selling BTC. If specified, the order book will be pruned down to only
         show orders at and above this fee_provided
        """
        base_asset_info = mongo_db.tracked_assets.find_one({'asset': base_asset})
        quote_asset_info = mongo_db.tracked_assets.find_one({'asset': quote_asset})
        
        if not base_asset_info or not quote_asset_info:
            raise Exception("Invalid asset(s)")
        
        #TODO: limit # results to 8 or so for each book (we have to sort as well to limit)
        base_bid_filters = [
            {"field": "get_asset", "op": "==", "value": base_asset},
            {"field": "give_asset", "op": "==", "value": quote_asset},
        ]
        base_ask_filters = [
            {"field": "get_asset", "op": "==", "value": quote_asset},
            {"field": "give_asset", "op": "==", "value": base_asset},
        ]
        if base_asset == 'BTC' or quote_asset == 'BTC':
            extra_filters = [
                {'field': 'give_remaining', 'op': '>', 'value': 0}, #don't show empty BTC orders
                {'field': 'get_remaining', 'op': '>', 'value': 0}, #don't show empty BTC orders
                {'field': 'fee_required_remaining', 'op': '>=', 'value': 0},
                {'field': 'fee_provided_remaining', 'op': '>=', 'value': 0},
            ]
            base_bid_filters += extra_filters
            base_ask_filters += extra_filters
        
        base_bid_orders = util.call_jsonrpc_api("get_orders", {
             'filters': base_bid_filters,
             'show_expired': False,
             'status': 'open',
             'order_by': 'block_index',
             'order_dir': 'asc',
            }, abort_on_error=True)['result']

        base_ask_orders = util.call_jsonrpc_api("get_orders", {
             'filters': base_ask_filters,
             'show_expired': False,
             'status': 'open',
             'order_by': 'block_index',
             'order_dir': 'asc',
            }, abort_on_error=True)['result']
        
        def get_o_pct(o):
            if o['give_asset'] == 'BTC': #NB: fee_provided could be zero here
                pct_fee_provided = float(( D(o['fee_provided_remaining']) / D(o['give_quantity']) ).quantize(
                            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
            else: pct_fee_provided = None
            if o['get_asset'] == 'BTC': #NB: fee_required could be zero here
                pct_fee_required = float(( D(o['fee_required_remaining']) / D(o['get_quantity']) ).quantize(
                            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
            else: pct_fee_required = None
            return pct_fee_provided, pct_fee_required

        #filter results by pct_fee_provided and pct_fee_required for BTC pairs as appropriate
        filtered_base_bid_orders = []
        filtered_base_ask_orders = []
        if base_asset == 'BTC' or quote_asset == 'BTC':      
            for o in base_bid_orders:
                pct_fee_provided, pct_fee_required = get_o_pct(o)
                addToBook = True
                if bid_book_min_pct_fee_provided is not None and pct_fee_provided is not None and pct_fee_provided < bid_book_min_pct_fee_provided:
                    addToBook = False
                if bid_book_min_pct_fee_required is not None and pct_fee_required is not None and pct_fee_required < bid_book_min_pct_fee_required:
                    addToBook = False
                if bid_book_max_pct_fee_required is not None and pct_fee_required is not None and pct_fee_required > bid_book_max_pct_fee_required:
                    addToBook = False
                if addToBook: filtered_base_bid_orders.append(o)
            for o in base_ask_orders:
                pct_fee_provided, pct_fee_required = get_o_pct(o)
                addToBook = True
                if ask_book_min_pct_fee_provided is not None and pct_fee_provided is not None and pct_fee_provided < ask_book_min_pct_fee_provided:
                    addToBook = False
                if ask_book_min_pct_fee_required is not None and pct_fee_required is not None and pct_fee_required < ask_book_min_pct_fee_required:
                    addToBook = False
                if ask_book_max_pct_fee_required is not None and pct_fee_required is not None and pct_fee_required > ask_book_max_pct_fee_required:
                    addToBook = False
                if addToBook: filtered_base_ask_orders.append(o)
        else:
            filtered_base_bid_orders += base_bid_orders
            filtered_base_ask_orders += base_ask_orders


        def make_book(orders, isBidBook):
            book = {}
            for o in orders:
                if o['give_asset'] == base_asset:
                    if base_asset == 'BTC' and o['give_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF:
                        continue #filter dust orders, if necessary
                    
                    give_quantity = util.normalize_quantity(o['give_quantity'], base_asset_info['divisible'])
                    get_quantity = util.normalize_quantity(o['get_quantity'], quote_asset_info['divisible'])
                    unit_price = float(( D(get_quantity) / D(give_quantity) ).quantize(
                        D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
                    remaining = util.normalize_quantity(o['give_remaining'], base_asset_info['divisible'])
                else:
                    if quote_asset == 'BTC' and o['give_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF:
                        continue #filter dust orders, if necessary

                    give_quantity = util.normalize_quantity(o['give_quantity'], quote_asset_info['divisible'])
                    get_quantity = util.normalize_quantity(o['get_quantity'], base_asset_info['divisible'])
                    unit_price = float(( D(give_quantity) / D(get_quantity) ).quantize(
                        D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
                    remaining = util.normalize_quantity(o['get_remaining'], base_asset_info['divisible'])
                id = "%s_%s_%s" % (base_asset, quote_asset, unit_price)
                #^ key = {base}_{bid}_{unit_price}, values ref entries in book
                book.setdefault(id, {'unit_price': unit_price, 'quantity': 0, 'count': 0})
                book[id]['quantity'] += remaining #base quantity outstanding
                book[id]['count'] += 1 #num orders at this price level
            book = sorted(book.itervalues(), key=operator.itemgetter('unit_price'), reverse=isBidBook)
            #^ convert to list and sort -- bid book = descending, ask book = ascending
            return book
        
        #compile into a single book, at volume tiers
        base_bid_book = make_book(filtered_base_bid_orders, True)
        base_ask_book = make_book(filtered_base_ask_orders, False)

        #get stats like the spread and median
        if base_bid_book and base_ask_book:
            #don't do abs(), as this is "the amount by which the ask price exceeds the bid", so I guess it could be negative
            # if there is overlap in the book (right?)
            bid_ask_spread = float(( D(base_ask_book[0]['unit_price']) - D(base_bid_book[0]['unit_price']) ).quantize(
                            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
            bid_ask_median = float(( D( max(base_ask_book[0]['unit_price'], base_bid_book[0]['unit_price']) ) - (D(abs(bid_ask_spread)) / 2) ).quantize(
                            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        else:
            bid_ask_spread = 0
            bid_ask_median = 0
        
        #compose depth and round out quantities
        bid_depth = D(0)
        for o in base_bid_book:
            o['quantity'] = float(D(o['quantity']).quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
            bid_depth += D(o['quantity'])
            o['depth'] = float(bid_depth.quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        bid_depth = float(bid_depth.quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        ask_depth = D(0)
        for o in base_ask_book:
            o['quantity'] = float(D(o['quantity']).quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
            ask_depth += D(o['quantity'])
            o['depth'] = float(ask_depth.quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        ask_depth = float(ask_depth.quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        
        #compose raw orders
        orders = filtered_base_bid_orders + filtered_base_ask_orders
        for o in orders:
            #add in the blocktime to help makes interfaces more user-friendly (i.e. avoid displaying block
            # indexes and display datetimes instead)
            o['block_time'] = time.mktime(util.get_block_time(o['block_index']).timetuple()) * 1000
            
        #for orders where BTC is the give asset, also return online status of the user
        for o in orders:
            if o['give_asset'] == 'BTC':
                r = mongo_db.btc_open_orders.find_one({'order_tx_hash': o['tx_hash']})
                o['_is_online'] = (r['wallet_id'] in siofeeds.onlineClients) if r else False
            else:
                o['_is_online'] = None #does not apply in this case

        result = {
            'base_bid_book': base_bid_book,
            'base_ask_book': base_ask_book,
            'bid_depth': bid_depth,
            'ask_depth': ask_depth,
            'bid_ask_spread': bid_ask_spread,
            'bid_ask_median': bid_ask_median,
            'raw_orders': orders,
        }
        return result
    
    @dispatcher.add_method
    def get_order_book_simple(asset1, asset2, min_pct_fee_provided=None, max_pct_fee_required=None):
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        result = _get_order_book(base_asset, quote_asset,
            bid_book_min_pct_fee_provided=min_pct_fee_provided,
            bid_book_max_pct_fee_required=max_pct_fee_required,
            ask_book_min_pct_fee_provided=min_pct_fee_provided,
            ask_book_max_pct_fee_required=max_pct_fee_required)
        return result

    @dispatcher.add_method
    def get_order_book_buysell(buy_asset, sell_asset, pct_fee_provided=None, pct_fee_required=None):
        base_asset, quote_asset = util.assets_to_asset_pair(buy_asset, sell_asset)
        bid_book_min_pct_fee_provided = None
        bid_book_min_pct_fee_required = None
        bid_book_max_pct_fee_required = None
        ask_book_min_pct_fee_provided = None
        ask_book_min_pct_fee_required = None
        ask_book_max_pct_fee_required = None
        if base_asset == 'BTC':
            if buy_asset == 'BTC':
                #if BTC is base asset and we're buying it, we're buying the BASE. we require a BTC fee (we're on the bid (bottom) book and we want a lower price)
                # - show BASE buyers (bid book) that require a BTC fee >= what we require (our side of the book)
                # - show BASE sellers (ask book) that provide a BTC fee >= what we require
                bid_book_min_pct_fee_required = pct_fee_required #my competition at the given fee required
                ask_book_min_pct_fee_provided = pct_fee_required
            elif sell_asset == 'BTC':
                #if BTC is base asset and we're selling it, we're selling the BASE. we provide a BTC fee (we're on the ask (top) book and we want a higher price)
                # - show BASE buyers (bid book) that provide a BTC fee >= what we provide 
                # - show BASE sellers (ask book) that require a BTC fee <= what we provide (our side of the book)
                bid_book_max_pct_fee_required = pct_fee_provided
                ask_book_min_pct_fee_provided = pct_fee_provided #my competition at the given fee provided
        elif quote_asset == 'BTC':
            assert base_asset == 'XCP' #only time when this is the case
            if buy_asset == 'BTC':
                #if BTC is quote asset and we're buying it, we're selling the BASE. we require a BTC fee (we're on the ask (top) book and we want a higher price)
                # - show BASE buyers (bid book) that provide a BTC fee >= what we require 
                # - show BASE sellers (ask book) that require a BTC fee >= what we require (our side of the book)
                bid_book_min_pct_fee_provided = pct_fee_required
                ask_book_min_pct_fee_required = pct_fee_required #my competition at the given fee required
            elif sell_asset == 'BTC':
                #if BTC is quote asset and we're selling it, we're buying the BASE. we provide a BTC fee (we're on the bid (bottom) book and we want a lower price)
                # - show BASE buyers (bid book) that provide a BTC fee >= what we provide (our side of the book)
                # - show BASE sellers (ask book) that require a BTC fee <= what we provide 
                bid_book_min_pct_fee_provided = pct_fee_provided #my compeitition at the given fee provided
                ask_book_max_pct_fee_required = pct_fee_provided

        result = _get_order_book(base_asset, quote_asset,
            bid_book_min_pct_fee_provided=bid_book_min_pct_fee_provided,
            bid_book_min_pct_fee_required=bid_book_min_pct_fee_required,
            bid_book_max_pct_fee_required=bid_book_max_pct_fee_required,
            ask_book_min_pct_fee_provided=ask_book_min_pct_fee_provided,
            ask_book_min_pct_fee_required=ask_book_min_pct_fee_required,
            ask_book_max_pct_fee_required=ask_book_max_pct_fee_required)
        
        #filter down raw_orders to be only open sell orders for what the caller is buying
        open_sell_orders = []
        for o in result['raw_orders']:
            if o['give_asset'] == buy_asset:
                open_sell_orders.append(o)
        result['raw_orders'] = open_sell_orders
        return result
    
    @dispatcher.add_method
    def get_transaction_stats(start_ts=None, end_ts=None):
        if not end_ts: #default to current datetime
            end_ts = time.mktime(datetime.datetime.utcnow().timetuple())
        if not start_ts: #default to 30 days before the end date
            start_ts = end_ts - (30 * 24 * 60 * 60)
                
        stats = mongo_db.transaction_stats.aggregate([
            {"$match": {
                "block_time": {
                    "$gte": datetime.datetime.utcfromtimestamp(start_ts),
                    "$lte": datetime.datetime.utcfromtimestamp(end_ts)
                }
            }},
            {"$project": {
                "year":  {"$year": "$block_time"},
                "month": {"$month": "$block_time"},
                "day":   {"$dayOfMonth": "$block_time"},
                "category": 1,
            }},
            {"$group": {
                "_id":   {"year": "$year", "month": "$month", "day": "$day", "category": "$category"},
                "count": {"$sum": 1},
            }}
            #{"$sort": SON([("_id.year", pymongo.ASCENDING), ("_id.month", pymongo.ASCENDING), ("_id.day", pymongo.ASCENDING), ("_id.hour", pymongo.ASCENDING), ("_id.category", pymongo.ASCENDING)])},
        ])
        times = {}
        categories = {}
        stats = [] if not stats['ok'] else stats['result']
        for e in stats:
            categories.setdefault(e['_id']['category'], {})
            time_val = int(time.mktime(datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']).timetuple()) * 1000)
            times.setdefault(time_val, True)
            categories[e['_id']['category']][time_val] = e['count']
        times_list = times.keys()
        times_list.sort()
        #fill in each array with all found timestamps
        for e in categories:
            a = []
            for t in times_list:
                a.append([t, categories[e][t] if t in categories[e] else 0])
            categories[e] = a #replace with array data
        #take out to final data structure
        categories_list = []
        for k, v in categories.iteritems():
            categories_list.append({'name': k, 'data': v})
        return categories_list
    
    @dispatcher.add_method
    def get_wallet_stats(start_ts=None, end_ts=None):
        if not end_ts: #default to current datetime
            end_ts = time.mktime(datetime.datetime.utcnow().timetuple())
        if not start_ts: #default to 30 days before the end date
            start_ts = end_ts - (30 * 24 * 60 * 60)
            
        num_wallets_mainnet = mongo_db.preferences.find({'network': 'mainnet'}).count()
        num_wallets_testnet = mongo_db.preferences.find({'network': 'testnet'}).count()
        num_wallets_unknown = mongo_db.preferences.find({'network': None}).count()
        wallet_stats = []
        for net in ['mainnet', 'testnet']:
            stats = mongo_db.wallet_stats.find(
                    {'when': {
                        "$gte": datetime.datetime.utcfromtimestamp(start_ts),
                        "$lte": datetime.datetime.utcfromtimestamp(end_ts)
                     }, 'network': net }).sort('when', pymongo.ASCENDING)
            new_wallet_counts = []
            login_counts = []
            distinct_login_counts = []
            for e in stats:
                d = int(time.mktime(datetime.datetime(e['when'].year, e['when'].month, e['when'].day).timetuple()) * 1000)
                distinct_login_counts.append([ d, e['distinct_login_count'] ])
                login_counts.append([ d, e['login_count'] ])
                new_wallet_counts.append([ d, e['new_count'] ])
            wallet_stats.append({'name': '%s: Logins' % net.capitalize(), 'data': login_counts})
            wallet_stats.append({'name': '%s: Active Wallets' % net.capitalize(), 'data': distinct_login_counts})
            wallet_stats.append({'name': '%s: New Wallets' % net.capitalize(), 'data': new_wallet_counts})
        return {
            'num_wallets_mainnet': num_wallets_mainnet,
            'num_wallets_testnet': num_wallets_testnet,
            'num_wallets_unknown': num_wallets_unknown,
            'wallet_stats': wallet_stats}
    
    @dispatcher.add_method
    def get_owned_assets(addresses):
        """Gets a list of owned assets for one or more addresses"""
        result = mongo_db.tracked_assets.find({
            'owner': {"$in": addresses}
        }, {"_id":0}).sort("asset", pymongo.ASCENDING)
        return list(result)
    
    @dispatcher.add_method
    def get_asset_pair_market_info(asset1=None, asset2=None, limit=50):
        """Given two arbitrary assets, returns the base asset and the quote asset.
        """
        assert (asset1 and asset2) or (asset1 is None and asset2 is None)
        if asset1 and asset2:
            base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
            pair_info = mongo_db.asset_pair_market_info.find({'base_asset': base_asset, 'quote_asset': quote_asset}, {'_id': 0})
        else:
            pair_info = mongo_db.asset_pair_market_info.find({}, {'_id': 0}).sort('completed_trades_count', pymongo.DESCENDING).limit(limit)
            #^ sort by this for now, may want to sort by a market_cap value in the future
        return list(pair_info) or []

    @dispatcher.add_method
    def get_asset_extended_info(asset):
        ext_info = mongo_db.asset_extended_info.find_one({'asset': asset}, {'_id': 0})
        return ext_info or False
    
    @dispatcher.add_method
    def get_asset_history(asset, reverse=False):
        """
        Returns a list of changes for the specified asset, from its inception to the current time.
        
        @param asset: The asset to retrieve a history on
        @param reverse: By default, the history is returned in the order of oldest to newest. Set this parameter to True
        to return items in the order of newest to oldest.
        
        @return:
        Changes are returned as a list of dicts, with each dict having the following format:
        * type: One of 'created', 'issued_more', 'changed_description', 'locked', 'transferred', 'called_back'
        * 'at_block': The block number this change took effect
        * 'at_block_time': The block time this change took effect
        
        * IF type = 'created': Has the following fields, as specified when the asset was initially created:
          * owner, description, divisible, locked, total_issued, total_issued_normalized
        * IF type = 'issued_more':
          * 'additional': The additional quantity issued (raw)
          * 'additional_normalized': The additional quantity issued (normalized)
          * 'total_issued': The total issuance after this change (raw)
          * 'total_issued_normalized': The total issuance after this change (normalized)
        * IF type = 'changed_description':
          * 'prev_description': The old description
          * 'new_description': The new description
        * IF type = 'locked': NO EXTRA FIELDS
        * IF type = 'transferred':
          * 'prev_owner': The address the asset was transferred from
          * 'new_owner': The address the asset was transferred to
        * IF type = 'called_back':
          * 'percentage': The percentage of the asset called back (between 0 and 100)
        """
        asset = mongo_db.tracked_assets.find_one({ 'asset': asset }, {"_id":0})
        if not asset:
            raise Exception("Unrecognized asset")
        
        #run down through _history and compose a diff log
        history = []
        raw = asset['_history'] + [asset,] #oldest to newest. add on the current state
        prev = None
        for i in xrange(len(raw)): #oldest to newest
            if i == 0:
                assert raw[i]['_change_type'] == 'created'
                history.append({
                    'type': 'created',
                    'owner': raw[i]['owner'],
                    'description': raw[i]['description'],
                    'divisible': raw[i]['divisible'],
                    'locked': raw[i]['locked'],
                    'total_issued': raw[i]['total_issued'],
                    'total_issued_normalized': raw[i]['total_issued_normalized'],
                    'at_block': raw[i]['_at_block'],
                    'at_block_time': time.mktime(raw[i]['_at_block_time'].timetuple()) * 1000,
                })
                prev = raw[i]
                continue
            
            assert prev
            if raw[i]['_change_type'] == 'locked':
                history.append({
                    'type': 'locked',
                    'at_block': raw[i]['_at_block'],
                    'at_block_time': time.mktime(raw[i]['_at_block_time'].timetuple()) * 1000,
                })
            elif raw[i]['_change_type'] == 'transferred':
                history.append({
                    'type': 'transferred',
                    'at_block': raw[i]['_at_block'],
                    'at_block_time': time.mktime(raw[i]['_at_block_time'].timetuple()) * 1000,
                    'prev_owner': prev['owner'],
                    'new_owner': raw[i]['owner'],
                })
            elif raw[i]['_change_type'] == 'changed_description':
                history.append({
                    'type': 'changed_description',
                    'at_block': raw[i]['_at_block'],
                    'at_block_time': time.mktime(raw[i]['_at_block_time'].timetuple()) * 1000,
                    'prev_description': prev['description'],
                    'new_description': raw[i]['description'],
                })
            else: #issue additional
                assert raw[i]['total_issued'] - prev['total_issued'] > 0
                history.append({
                    'type': 'issued_more',
                    'at_block': raw[i]['_at_block'],
                    'at_block_time': time.mktime(raw[i]['_at_block_time'].timetuple()) * 1000,
                    'additional': raw[i]['total_issued'] - prev['total_issued'],
                    'additional_normalized': raw[i]['total_issued_normalized'] - prev['total_issued_normalized'],
                    'total_issued': raw[i]['total_issued'],
                    'total_issued_normalized': raw[i]['total_issued_normalized'],
                })
            prev = raw[i]
        
        #get callbacks externally via the cpd API, and merge in with the asset history we composed
        callbacks = util.call_jsonrpc_api("get_callbacks",
            {'filters': {'field': 'asset', 'op': '==', 'value': asset['asset']}}, abort_on_error=True)['result']
        final_history = []
        if len(callbacks):
            for e in history: #history goes from earliest to latest
                if callbacks[0]['block_index'] < e['at_block']: #throw the callback entry in before this one
                    block_time = util.get_block_time(callbacks[0]['block_index'])
                    assert block_time
                    final_history.append({
                        'type': 'called_back',
                        'at_block': callbacks[0]['block_index'],
                        'at_block_time': time.mktime(block_time.timetuple()) * 1000,
                        'percentage': callbacks[0]['fraction'] * 100,
                    })
                    callbacks.pop(0)
                else:
                    final_history.append(e)
        else:
            final_history = history
        if reverse: final_history.reverse()
        return final_history

    @dispatcher.add_method
    def record_btc_open_order(wallet_id, order_tx_hash):
        """Records an association between a wallet ID and order TX ID for a trade where BTC is being SOLD, to allow
        buyers to see which sellers of the BTC are "online" (which can lead to a better result as a BTCpay will be required
        to complete any trades where BTC is involved, and the seller (or at least their wallet) must be online for this to happen"""
        #ensure the wallet_id exists
        result =  mongo_db.preferences.find_one({"wallet_id": wallet_id})
        if not result: raise Exception("WalletID does not exist")
        
        mongo_db.btc_open_orders.insert({
            'wallet_id': wallet_id,
            'order_tx_hash': order_tx_hash,
            'when_created': datetime.datetime.utcnow()
        })
        return True

    @dispatcher.add_method
    def cancel_btc_open_order(wallet_id, order_tx_hash):
        mongo_db.btc_open_orders.remove({'order_tx_hash': order_tx_hash, 'wallet_id': wallet_id})
        #^ wallet_id is used more for security here so random folks can't remove orders from this collection just by tx hash
        return True
    
    @dispatcher.add_method
    def get_balance_history(asset, addresses, normalize=True, start_ts=None, end_ts=None):
        """Retrieves the ordered balance history for a given address (or list of addresses) and asset pair, within the specified date range
        @param normalize: If set to True, return quantities that (if the asset is divisible) have been divided by 100M (satoshi). 
        @return: A list of tuples, with the first entry of each tuple being the block time (epoch TS), and the second being the new balance
         at that block time.
        """
        if not isinstance(addresses, list):
            raise Exception("addresses must be a list of addresses, even if it just contains one address")
            
        asset_info = mongo_db.tracked_assets.find_one({'asset': asset})
        if not asset_info:
            raise Exception("Asset does not exist.")
            
        if not end_ts: #default to current datetime
            end_ts = time.mktime(datetime.datetime.utcnow().timetuple())
        if not start_ts: #default to 30 days before the end date
            start_ts = end_ts - (30 * 24 * 60 * 60)
        results = []
        for address in addresses:
            result = mongo_db.balance_changes.find({
                'address': address,
                'asset': asset,
                "block_time": {
                    "$gte": datetime.datetime.utcfromtimestamp(start_ts),
                    "$lte": datetime.datetime.utcfromtimestamp(end_ts)
                }
            }).sort("block_time", pymongo.ASCENDING)
            results.append({
                'name': address,
                'data': [
                    (time.mktime(r['block_time'].timetuple()) * 1000,
                     r['new_balance_normalized'] if normalize else r['new_balance']
                    ) for r in result]
            })
        return results

    @dispatcher.add_method
    def get_num_users_online():
        #gets the current number of users attached to the server's chat feed
        return len(siofeeds.onlineClients) 

    @dispatcher.add_method
    def is_chat_handle_in_use(handle):
        results = mongo_db.chat_handles.find({ 'handle': { '$regex': '^%s$' % handle, '$options': 'i' } })
        return True if results.count() else False 

    @dispatcher.add_method
    def get_chat_handle(wallet_id):
        result = mongo_db.chat_handles.find_one({"wallet_id": wallet_id})
        if not result: return False #doesn't exist
        result['last_touched'] = time.mktime(time.gmtime())
        mongo_db.chat_handles.save(result)
        data = {
            'handle': re.sub('[^A-Za-z0-9_-]', "", result['handle']),
            'is_op': result.get('is_op', False),
            'last_updated': result.get('last_updated', None)
            } if result else {}
        banned_until = result.get('banned_until', None) 
        if banned_until != -1 and banned_until is not None:
            data['banned_until'] = int(time.mktime(banned_until.timetuple())) * 1000 #convert to epoch ts in ms
        else:
            data['banned_until'] = banned_until #-1 or None
        return data

    @dispatcher.add_method
    def store_chat_handle(wallet_id, handle):
        """Set or update a chat handle"""
        if not isinstance(handle, basestring):
            raise Exception("Invalid chat handle: bad data type")
        if not re.match(r'^[A-Za-z0-9_-]{4,12}$', handle):
            raise Exception("Invalid chat handle: bad syntax/length")
        
        #see if this handle already exists (case insensitive)
        results = mongo_db.chat_handles.find({ 'handle': { '$regex': '^%s$' % handle, '$options': 'i' } })
        if results.count():
            if results[0]['wallet_id'] == wallet_id:
                return True #handle already saved for this wallet ID
            else:
                raise Exception("Chat handle already is in use")

        mongo_db.chat_handles.update(
            {'wallet_id': wallet_id},
            {"$set": {
                'wallet_id': wallet_id,
                'handle': handle,
                'last_updated': time.mktime(time.gmtime()),
                'last_touched': time.mktime(time.gmtime()) 
                }
            }, upsert=True)
        #^ last_updated MUST be in UTC, as it will be compaired again other servers
        return True

    @dispatcher.add_method
    def get_chat_history(start_ts=None, end_ts=None, handle=None, limit=1000):
        if not end_ts: #default to current datetime
            end_ts = time.mktime(datetime.datetime.utcnow().timetuple())
        if not start_ts: #default to 5 days before the end date
            start_ts = end_ts - (30 * 24 * 60 * 60)
            
        if limit >= 5000:
            raise Exception("Requesting too many lines (limit too high")
        
        filters = {
            "when": {
                "$gte": datetime.datetime.utcfromtimestamp(start_ts),
                "$lte": datetime.datetime.utcfromtimestamp(end_ts)
            }
        }
        if handle:
            filters['handle'] = handle
        chat_history = mongo_db.chat_history.find(filters, {'_id': 0}).sort("when", pymongo.DESCENDING).limit(limit)
        if not chat_history.count():
            return False #no suitable trade data to form a market price
        chat_history = list(chat_history)
        return chat_history 

    @dispatcher.add_method
    def is_wallet_online(wallet_id):
        return wallet_id in siofeeds.onlineClients

    @dispatcher.add_method
    def get_preferences(wallet_id, for_login=False, network=None):
        """Gets stored wallet preferences
        @param network: only required if for_login is specified. One of: 'mainnet' or 'testnet'
        """
        if network not in (None, 'mainnet', 'testnet'):
            raise Exception("Invalid network parameter setting")
        if for_login and network is None:
            raise Exception("network parameter required if for_login is set")

        result =  mongo_db.preferences.find_one({"wallet_id": wallet_id})
        if not result: return False #doesn't exist
        
        last_touched_date = datetime.datetime.utcfromtimestamp(result['last_touched']).date()
        now = datetime.datetime.utcnow()
         
        if for_login: #record user login
            mongo_db.login_history.insert({'wallet_id': wallet_id, 'when': now, 'network': network, 'action': 'login'})
        
        result['last_touched'] = time.mktime(time.gmtime())
        mongo_db.preferences.save(result)

        return {
            'preferences': json.loads(result['preferences']),
            'last_updated': result.get('last_updated', None)
        } 


    @dispatcher.add_method
    def store_preferences(wallet_id, preferences, for_login=False, network=None, referer=None):
        """Stores freeform wallet preferences
        @param network: only required if for_login is specified. One of: 'mainnet' or 'testnet'
        """
        if network not in (None, 'mainnet', 'testnet'):
            raise Exception("Invalid network parameter setting")
        if for_login and network is None:
            raise Exception("network parameter required if for_login is set")
        if not isinstance(preferences, dict):
            raise Exception("Invalid preferences object")
        try:
            preferences_json = json.dumps(preferences)
        except:
            raise Exception("Cannot dump preferences to JSON")
        
        now = datetime.datetime.utcnow()
        
        #sanity check around max size
        if len(preferences_json) >= PREFERENCES_MAX_LENGTH:
            raise Exception("Preferences object is too big.")
        
        if for_login: #mark this as a new signup
            mongo_db.login_history.insert({'wallet_id': wallet_id, 'when': now, 'network': network, 'action': 'create', 'referer': referer})
        
        now_ts = time.mktime(time.gmtime())
        mongo_db.preferences.update(
            {'wallet_id': wallet_id},
            {'$set': {
                'wallet_id': wallet_id,
                'preferences': preferences_json,
                'last_updated': now_ts,
                'last_touched': now_ts },
             '$setOnInsert': {'when_created': now_ts, 'network': network}
            }, upsert=True)
        #^ last_updated MUST be in GMT, as it will be compaired again other servers
        return True
    
    @dispatcher.add_method
    def proxy_to_counterpartyd(method='', params=[]):
        if method=='sql': raise Exception("Invalid method") 
        result = None
        cache_key = None

        if redis_client: #check for a precached result and send that back instead
            cache_key = method + '||' + base64.b64encode(json.dumps(params).encode()).decode()
            #^ must use encoding (e.g. base64) since redis doesn't allow spaces in its key names
            # (also shortens the hashing key for better performance)
            result = redis_client.get(cache_key)
            if result:
                try:
                    result = json.loads(result)
                except Exception, e:
                    logging.warn("Error loading JSON from cache: %s, cached data: '%s'" % (e, result))
                    result = None #skip from reading from cache and just make the API call
        
        if result is None: #cache miss or cache disabled
            result = util.call_jsonrpc_api(method, params)
            if redis_client: #cache miss
                redis_client.setex(cache_key, DEFAULT_COUNTERPARTYD_API_CACHE_PERIOD, json.dumps(result))
                #^TODO: we may want to have different cache periods for different types of data
        
        if 'error' in result:
            if result['error'].get('data', None):
                errorMsg = result['error']['data'].get('message', result['error']['message'])
            else:
                errorMsg = json.dumps(result['error'])
            raise Exception(errorMsg.encode('ascii','ignore'))
            #decode out unicode for now (json-rpc lib was made for python 3.3 and does str(errorMessage) internally,
            # which messes up w/ unicode under python 2.x)
        return result['result']

    @dispatcher.add_method
    def get_bets(bet_type, feed_address, deadline, target_value=None, leverage=5040):
        bets = betting.find_bets(bet_type, feed_address, deadline, target_value=target_value, leverage=leverage)
        return bets

    @dispatcher.add_method
    def get_user_bets(addresses = [], status="open"):
        bets = betting.find_user_bets(mongo_db, addresses, status)
        return bets

    @dispatcher.add_method
    def get_feed(address_or_url = ''):
        feed = betting.find_feed(mongo_db, address_or_url)
        return feed

    @dispatcher.add_method
    def get_feeds_by_source(addresses = []):
        feed = betting.get_feeds_by_source(mongo_db, addresses)
        return feed

    @dispatcher.add_method
    def parse_base64_feed(base64_feed):
        feed = betting.parse_base64_feed(base64_feed)
        return feed

    class API(object):
        @cherrypy.expose
        def index(self):
            if cherrypy.request.headers.get("Content-Type", None) == 'application/csp-report':
                try:
                    data_json = cherrypy.request.body.read().decode('utf-8')
                    data = json.loads(data_json)
                    assert 'csp-report' in data
                except Exception, e:
                    raise cherrypy.HTTPError(400, 'Invalid JSON document')
                tx_logger.info("***CSP SECURITY --- %s" % data_json)
                cherrypy.response.status = 200
                return ""
            
            cherrypy.response.headers["Content-Type"] = 'application/json'
            
            if cherrypy.request.method == "GET": #handle GET statistics checking
                #"ping" counterpartyd to test, as well
                try:
                    cpd_status = util.call_jsonrpc_api("get_running_info", [], abort_on_error=True)['result']
                except:
                    cherrypy.response.status = 500
                else:
                    cherrypy.response.status = 200
                result = {
                    'counterblockd': 'OK',
                    'counterpartyd': 'OK' if cherrypy.response.status == 200 else 'NOT OK'
                }
                return json.dumps(result)

            if config.ALLOW_CORS:
                cherrypy.response.headers['Access-Control-Allow-Origin'] = '*'
                cherrypy.response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
                cherrypy.response.headers['Access-Control-Allow-Headers'] = 'DNT,X-Mx-ReqToken,Keep-Alive,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type';            
                if cherrypy.request.method == "OPTIONS":
                    cherrypy.response.status = 204
                    return ""
            
            #don't do jack if we're not caught up
            if not util.is_caught_up_well_enough_for_government_work():
                raise cherrypy.HTTPError(525, 'Server is not caught up. Please try again later.')
                #^ 525 is a custom response code we use for this one purpose
            try:
                data_json = cherrypy.request.body.read().decode('utf-8')
            except ValueError:
                raise cherrypy.HTTPError(400, 'Invalid JSON document')
            response = JSONRPCResponseManager.handle(data_json, dispatcher)
            response_json = json.dumps(response.data, default=util.json_dthandler).encode()
            
            #log the request data to mongo
            try:
                data = json.loads(data_json)
                assert 'method' in data
                tx_logger.info("TRANSACTION --- %s ||| REQUEST: %s ||| RESPONSE: %s" % (data['method'], data_json, response_json))
            except Exception, e:
                logging.info("Could not log transaction: Invalid format: %s" % e)
                
            return response_json
        
    cherrypy.config.update({
        'log.screen': False,
        "environment": "embedded",
        'log.error_log.propagate': False,
        'log.access_log.propagate': False,
        "server.logToScreen" : False
    })
    app_config = {
        '/': {
            'tools.trailing_slash.on': False,
        },
        '/asset_img': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': os.path.join(config.data_dir, 'asset_img'),
            'tools.staticdir.content_types': {'png': 'image/png'}
        }
    }
    application = cherrypy.Application(API(), script_name="/api", config=app_config)

    #disable logging of the access and error logs to the screen
    application.log.access_log.propagate = False
    application.log.error_log.propagate = False
        
    #set up a rotating log handler for this application
    # Remove the default FileHandlers if present.
    application.log.error_file = ""
    application.log.access_file = ""
    maxBytes = getattr(application.log, "rot_maxBytes", 10000000)
    backupCount = getattr(application.log, "rot_backupCount", 1000)
    # Make a new RotatingFileHandler for the error log.
    fname = getattr(application.log, "rot_error_file", os.path.join(config.data_dir, "api.error.log"))
    h = logging_handlers.RotatingFileHandler(fname, 'a', maxBytes, backupCount)
    h.setLevel(logging.DEBUG)
    h.setFormatter(cherrypy._cplogging.logfmt)
    application.log.error_log.addHandler(h)
    # Make a new RotatingFileHandler for the access log.
    fname = getattr(application.log, "rot_access_file", os.path.join(config.data_dir, "api.access.log"))
    h = logging_handlers.RotatingFileHandler(fname, 'a', maxBytes, backupCount)
    h.setLevel(logging.DEBUG)
    h.setFormatter(cherrypy._cplogging.logfmt)
    application.log.access_log.addHandler(h)
    
    #start up the API listener/handler
    server = pywsgi.WSGIServer((config.RPC_HOST, int(config.RPC_PORT)), application, log=None)
    server.serve_forever()
