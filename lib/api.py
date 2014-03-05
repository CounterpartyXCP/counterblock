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
from logging import handlers as logging_handlers
import requests
from gevent import wsgi
import cherrypy
from cherrypy.process import plugins
from jsonrpc import JSONRPCResponseManager, dispatcher
import pymongo
from bson import json_util

from . import (config, util)

PREFERENCES_MAX_LENGTH = 100000 #in bytes, as expressed in JSON
D = decimal.Decimal

def get_block_indexes_for_dates(mongo_db, start_ts, end_ts):
    """Returns a 2 tuple (start_block, end_block) result for the block range that encompasses the given start_date
    and end_date unix timestamps"""
    start_block = mongo_db.processed_blocks.find_one({"block_time": {"$lte": start_ts} }, sort=[("block_time", pymongo.ASCENDING)])
    end_block = mongo_db.processed_blocks.find_one({"block_time": {"$gte": end_ts} }, sort=[("block_time", pymongo.ASCENDING)])
    start_block_index = config.BLOCK_FIRST if not start_block else start_block['block_index']
    if not end_block:
        end_block_index = mongo_db.processed_blocks.find_one(sort=[("block_index", pymongo.DESCENDING)])['block_index']
    else:
        end_block_index = end_block.block_index
    return (start_block_index, end_block_index)

def serve_api(mongo_db, redis_client):
    # Preferneces are just JSON objects... since we don't force a specific form to the wallet on
    # the server side, this makes it easier for 3rd party wallets (i.e. not counterwallet) to fully be able to
    # use counterwalletd to not only pull useful data, but also load and store their own preferences, containing
    # whatever data they need
    
    DEFAULT_COUNTERPARTYD_API_CACHE_PERIOD = 60 #in seconds
    
    @dispatcher.add_method
    def is_ready():
        """this method used by the client to check if the server is alive, caught up, and ready to accept requests.
        If the server is NOT caught up, a 525 error will be returned actually before hitting this point. Thus,
        if we actually return data from this function, it should always be true. (may change this behaviour later)"""
        assert config.CAUGHT_UP
        return {
            'caught_up': config.CAUGHT_UP,
            'testnet': config.TESTNET 
        }

    @dispatcher.add_method
    def get_normalized_balances(addresses):
        """ Does not retrieve BTC balance """
        if not isinstance(addresses, list):
            addresses = [addresses,]
        if not len(addresses):
            raise Exception("Invalid address list supplied")
        
        filters = []
        for address in addresses:
            filters.append({'field': 'address', 'op': '==', 'value': address})

        data = util.call_jsonrpc_api("get_balances",
            {'filters': filters, 'filterop': 'or'}, abort_on_error=True)['result']
        for d in data:
            asset_info = mongo_db.tracked_assets.find_one({'asset': d['asset']})
            d['normalized_amount'] = util.normalize_amount(d['amount'], asset_info['divisible'])
            
        return data 

    @dispatcher.add_method
    def get_raw_transactions(address, start_ts=None, end_ts=None, limit=10000):
        """Gets raw transactions for a particular address or set of addresses
        
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
        start_block_index, end_block_index = get_block_indexes_for_dates(mongo_db, start_ts, end_ts)
        
        #get all blocks (to derive block times) between the two block indexes
        blocks = mongo_db.processed_blocks.find(
            {"block_index": {"$gte": start_block_index, "$lte": end_block_index} }).sort("block_index", pymongo.ASCENDING)
        
        #make API call to counterpartyd to get all of the data for the specified address
        txns = []
        d = util.call_jsonrpc_api("get_address",
            {'address': address,
             'start_block': start_block_index,
             'end_block': end_block_index}, abort_on_error=True)['result']
        #mash it all together
        for k, v in d.iteritems():
            if k in ['balances', 'debits', 'credits']:
                continue
            if k in ['sends', 'callbacks']: #add asset divisibility info
                for e in v:
                    asset_info = get_asset_cached(e['asset'], asset_cache)
                    e['_divisible'] = asset_info['divisible']
            if k in ['orders',]: #add asset divisibility info for both assets
                for e in v:
                    give_asset_info = get_asset_cached(e['give_asset'], asset_cache)
                    e['_give_divisible'] = give_asset_info['divisible']
                    get_asset_info = get_asset_cached(e['get_asset'], asset_cache)
                    e['_get_divisible'] = get_asset_info['divisible']
            if k in ['order_matches',]: #add asset divisibility info for both assets
                for e in v:
                    forward_asset_info = get_asset_cached(e['forward_asset'], asset_cache)
                    e['_forward_divisible'] = forward_asset_info['divisible']
                    backward_asset_info = get_asset_cached(e['backward_asset'], asset_cache)
                    e['_backward_divisible'] = backward_asset_info['divisible']
            if k in ['bet_expirations', 'order_expirations', 'bet_match_expirations', 'order_match_expirations']:
                for e in v:
                    e['tx_index'] = 0 #add tx_index to all entries (so we can sort on it secondarily), since these lack it
            for e in v:
                e['_entity'] = k
                e['_block_time'] = blocks[e['block_index'] - start_block_index]['block_time']  
            txns += v
        txns = util.multikeysort(txns, ['-block_index', '-tx_index'])
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
        return _get_market_price_summary(asset1, asset2, with_last_trades)

    def _get_market_price_summary(asset1, asset2, with_last_trades=0):
        """Gets a synthesized trading "market price" for a specified asset pair (if available), as well as additional info.
        If no price is available, False is returned.
        """
        #look for the last max 6 trades within the past 10 day window
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        base_asset_info = mongo_db.tracked_assets.find_one({'asset': base_asset})
        quote_asset_info = mongo_db.tracked_assets.find_one({'asset': quote_asset})
        
        if not isinstance(with_last_trades, int) or with_last_trades < 0 or with_last_trades > 30:
            raise Exception("Invalid with_last_trades")
        
        if not base_asset_info or not quote_asset_info:
            raise Exception("Invalid asset(s)")
        
        min_trade_time = datetime.datetime.utcnow() - datetime.timedelta(days=10)
        last_trades = mongo_db.trades.find(
            {
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                'block_time': { "$gte": min_trade_time }
            },
            {'_id': 0, 'block_index': 1, 'block_time': 1, 'unit_price': 1, 'base_amount_normalized': 1, 'quote_amount_normalized': 1}
        ).sort("block_time", pymongo.DESCENDING).limit(with_last_trades)
        if not last_trades.count():
            return False #no suitable trade data to form a market price
        last_trades = list(last_trades)
        last_trades.reverse() #from newest to oldest
        weights = [1, .9, .72, .6, .4, .3] #good first guess...maybe
        weighted_inputs = []
        for i in xrange(6): #last 6 trades
            weighted_inputs.append([last_trades[i]['unit_price'], weights[i]])
        market_price = util.weighted_average(weighted_inputs)
        result = {
            'market_price': float(D(market_price).quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN)),
            'base_asset': base_asset,
            'quote_asset': quote_asset,
        }
        if with_last_trades:
            #[0]=block_time, [1]=unit_price, [2]=base_amount_normalized, [3]=quote_amount_normalized, [4]=block_index
            result['last_trades'] = [[
                t['block_time'],
                t['unit_price'],
                t['base_amount_normalized'],
                t['quote_amount_normalized'],
                t['block_index']
            ] for t in last_trades]
        return result
        
    @dispatcher.add_method
    def get_market_info(assets):
        """Returns information related to capitalization, volume, etc for the supplied asset(s)
        
        NOTE: in_btc == base asset is BTC, in_xcp == base asset is XCP
        
        @param assets: A list of one or more assets
        """
        def calc_inverse(amount):
            return float( (D(1) / D(amount) ).quantize(
                D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))            

        def calc_price_change(open, close):
            return float((D(100) * (D(close) - D(open)) / D(open)).quantize(
                    D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))            
        
        asset_data = {}
        start_dt_1d = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        start_dt_7d = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        mps_xcp_btc = _get_market_price_summary('XCP', 'BTC', with_last_trades=30)
        xcp_btc_price = mps_xcp_btc['market_price'] # == XCP/BTC
        btc_xcp_price = calc_inverse(mps_xcp_btc['market_price']) #BTC/XCP
        
        for asset in assets:
            asset_info = mongo_db.tracked_assets.find_one({'asset': asset})
            if asset == 'BTC':
                asset_info['total_issued'] = util.get_btc_supply(normalize=False)
                asset_info['total_issued_normalzied'] = util.normalize_amount(asset_info['total_issued'], True)
            elif asset == 'XCP':
                asset_info['total_issued'] = util.call_jsonrpc_api("get_xcp_supply", [], abort_on_error=True)['result']
                asset_info['total_issued_normalzied'] = util.normalize_amount(asset_info['total_issued'], True)
                
            if not asset_info:
                raise Exception("Invalid asset: %s" % asset)
            
            if asset not in ['BTC', 'XCP']:
                #get price data for both the asset with XCP, as well as BTC
                price_summary_in_xcp = _get_market_price_summary(asset, 'XCP', with_last_trades=30)
                price_summary_in_btc = _get_market_price_summary(asset, 'BTC', with_last_trades=30)
                #aggregated (averaged) price (expressed as XCP) for the asset on both the XCP and BTC markets
                aggregated_price_in_xcp = float(((D(price_summary_in_xcp['market_price']) + D(xcp_btc_price)) / D(2)).quantize(
                    D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
                aggregated_price_in_btc = float(((D(price_summary_in_btc['market_price']) + D(btc_xcp_price)) / D(2)).quantize(
                    D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
                price_in_xcp = price_summary_in_xcp['market_price']
                price_in_btc = price_summary_in_btc['market_price']
            else:
                #here we take the normal XCP/BTC pair, and invert it to BTC/XCP, to get XCP's data in terms of a BTC base
                # (this is the only area we do this, as BTC/XCP is NOT standard pair ordering)
                price_summary_in_xcp = mps_xcp_btc
                price_summary_in_btc = copy.deepcopy(mps_xcp_btc) #must invert this
                price_summary_in_btc['market_price'] = calc_inverse(price_summary_in_btc['market_price'])
                price_summary_in_btc['base_asset'] = 'BTC'
                price_summary_in_btc['quote_asset'] = 'XCP'
                for i in xrange(len(price_summary_in_btc['last_trades'])):
                    #[0]=block_time, [1]=unit_price, [2]=base_amount_normalized, [3]=quote_amount_normalized, [4]=block_index
                    price_summary_in_btc['last_trades'][i][1] = calc_inverse(price_summary_in_btc['last_trades'][i][1])
                    price_summary_in_btc['last_trades'][i][2], price_summary_in_btc['last_trades'][i][3] = \
                        price_summary_in_btc['last_trades'][i][3], price_summary_in_btc['last_trades'][i][2] #swap
                if asset == 'XCP':
                    aggregated_price_in_xcp = 1.0
                    aggregated_price_in_btc = btc_xcp_price
                    price_in_xcp = 1.0
                    price_in_btc = price_summary_in_btc['market_price']
                else:
                    assert asset == 'BTC'
                    aggregated_price_in_xcp = xcp_btc_price
                    aggregated_price_in_btc = 1.0
                    price_in_xcp = price_summary_in_xcp['market_price']
                    price_in_btc = 1.0
            
            #get XCP and BTC market summarized trades over a 7d period (quantize to 30 slots)
            history_in_xcp = None # xcp/asset market (or xcp/btc for xcp or btc)
            history_in_btc = None # btc/asset market (or btc/xcp for xcp or btc)
            if asset not in ['BTC', 'XCP']:
                for a in ['XCP', 'BTC']:
                    history = mongo_db.trades.aggregate([
                        {"$match": {
                            "base_asset": a,
                            "quote_asset": asset,
                            "block_time": {"$gte": start_dt_7d } }},
                        {"$project": {
                            "month": {"$month": "$block_time"},
                            "day":   {"$dayOfMonth": "$block_time"},
                            "hour":  {"$hour": "$block_time"},
                            "unit_price": 1,
                            "base_amount_normalized": 1 #to derive volume
                        }},
                        {"$sort": {"block_time": pymongo.ASCENDING}},
                        {"$group": {
                            "_id":   {"month": "$month", "day": "$day", "hour": "$hour"},
                            "price": {"$avg": "$unit_price"},
                            "vol":   {"$sum": "$base_amount_normalized"},
                        }},
                    ])
                    history = [] if not history['ok'] else history['result']
                    if a == 'XCP': history_in_xcp = history
                    else: history_in_btc = history
            else: #get the XCP/BTC market and invert for BTC/XCP (history_in_btc)
                history = mongo_db.trades.aggregate([
                    {"$match": {
                        "base_asset": 'XCP',
                        "quote_asset": 'BTC',
                        "block_time": {"$gte": start_dt_7d } }},
                    {"$project": {
                        "month": {"$month": "$block_time"},
                        "day":   {"$dayOfMonth": "$block_time"},
                        "hour":  {"$hour": "$block_time"},
                        "unit_price": 1,
                        "base_amount_normalized": 1 #to derive volume
                    }},
                    {"$sort": {"block_time": pymongo.ASCENDING}},
                    {"$group": {
                        "_id":   {"month": "$month", "day": "$day", "hour": "$hour"},
                        "price": {"$avg": "$unit_price"},
                        "vol":   {"$sum": "$base_amount_normalized"},
                    }},
                ])
                history = [] if not history['ok'] else history['result']
                history_in_xcp = history
                history_in_btc = copy.deepcopy(history_in_xcp)
                for i in xrange(len(history_in_btc)):
                    history_in_btc[i]['price'] = calc_inverse(history_in_btc[i]['price'])
                    history_in_btc[i]['vol'] = calc_inverse(history_in_btc[i]['vol'])
                
            #perform aggregation to get 24h statistics
            #TOTAL volume and count across all trades for the asset
            total_agg_result = {'vol': 0, 'count': 0}
            total_agg_result_as_base = mongo_db.trades.aggregate([
                {"$match": {
                    "base_asset": asset,
                    "block_time": {"$gte": start_dt_1d } }},
                {"$project": {
                    "base_amount_normalized": 1 #to derive volume
                }},
                {"$group": {
                    "_id":   1,
                    "vol":   {"$sum": "$base_amount_normalized"},
                    "count": {"$sum": 1},
                }}
            ])
            total_agg_result_as_base = {} if not total_agg_result_as_base['ok'] \
                or not len(total_agg_result_as_base['result']) else total_agg_result_as_base['result'][0]
            total_agg_result_as_quote = mongo_db.trades.aggregate([
                {"$match": {
                    "quote_asset": asset,
                    "block_time": {"$gte": start_dt_1d } }},
                {"$project": {
                    "quote_amount_normalized": 1 #to derive volume
                }},
                {"$group": {
                    "_id":   1,
                    "vol":   {"$sum": "quote_amount_normalized"},
                    "count": {"$sum": 1},
                }}
            ])
            total_agg_result_as_quote = {} if not total_agg_result_as_quote['ok'] \
                or not len(total_agg_result_as_quote['result']) else total_agg_result_as_quote['result'][0]
            total_agg_result['vol'] = total_agg_result_as_base.get('vol', 0) + total_agg_result_as_quote.get('vol', 0) 
            total_agg_result['count'] = total_agg_result_as_base.get('count', 0) + total_agg_result_as_quote.get('count', 0) 
            
            #XCP market volume with stats
            if asset != 'XCP' and len(price_summary_in_xcp['last_trades']):
                xcp_base_agg_result = mongo_db.trades.aggregate([
                    {"$match": {
                        "base_asset": "XCP",
                        "quote_asset": asset,
                        "block_time": {"$gte": start_dt_1d } }},
                    {"$project": {
                        "unit_price": 1,
                        "base_amount_normalized": 1 #to derive volume
                    }},
                    {"$group": {
                        "_id":   1,
                        "open":  {"$first": "$unit_price"},
                        "high":  {"$max": "$unit_price"},
                        "low":   {"$min": "$unit_price"},
                        "close": {"$last": "$unit_price"},
                        "vol":   {"$sum": "$base_amount_normalized"},
                        "count": {"$sum": 1},
                    }}
                ])
                xcp_base_agg_result = [] if not xcp_base_agg_result['ok'] \
                    or not len(xcp_base_agg_result['result']) else xcp_base_agg_result['result'][0]
                if xcp_base_agg_result: del xcp_base_agg_result['id']
            else:
                xcp_base_agg_result = []
                
            #BTC market volume with stats
            if asset != 'BTC' and len(price_summary_in_btc['last_trades']):
                btc_base_agg_result = mongo_db.trades.aggregate([
                    {"$match": {
                        "base_asset": "BTC",
                        "quote_asset": asset,
                        "block_time": {"$gte": start_dt_1d } }},
                    {"$project": {
                        "unit_price": 1,
                        "base_amount_normalized": 1 #to derive volume
                    }},
                    {"$group": {
                        "_id":   1,
                        "open":  {"$first": "$unit_price"},
                        "high":  {"$max": "$unit_price"},
                        "low":   {"$min": "$unit_price"},
                        "close": {"$last": "$unit_price"},
                        "vol":   {"$sum": "$base_amount_normalized"},
                        "count": {"$sum": 1},
                    }}
                ])
                btc_base_agg_result = [] if not btc_base_agg_result['ok'] \
                    or not len(btc_base_agg_result['result']) else btc_base_agg_result['result'][0]
                if btc_base_agg_result: del btc_base_agg_result['id']
            else:
                btc_base_agg_result = []
            
            asset_data[asset] = {
                'price_in_xcp': price_in_xcp, #current price of asset against XCP
                'price_in_btc': price_in_btc, #current price of asset against BTC
                'aggregated_price_in_xcp': aggregated_price_in_xcp, 
                'aggregated_price_in_btc': aggregated_price_in_btc,
                'total_supply': asset_info['total_issued_normalzied'], 
                'market_cap_in_xcp': float( (D(asset_info['total_issued_normalzied']) / D(price_in_xcp)).quantize(
                    D('.00000000'), rounding=decimal.ROUND_HALF_EVEN) ),
                'market_cap_in_btc': float( (D(asset_info['total_issued_normalzied']) / D(price_in_btc)).quantize(
                    D('.00000000'), rounding=decimal.ROUND_HALF_EVEN) ),
                '24h_summary': total_agg_result,
                #^ total amount traded of that asset in all markets in last 24h
                '24h_summary_in_xcp': xcp_base_agg_result,
                #^ amount of asset traded with BTC in last 24h
                '24h_summary_in_btc': btc_base_agg_result,
                #^ amount of asset traded with XCP in last 24h
                '24h_vol_price_change_in_xcp': calc_price_change(xcp_base_agg_result['open'], xcp_base_agg_result['close'])
                    if xcp_base_agg_result else None,
                #^ aggregated price change from 24h ago to now, expressed as a signed float (e.g. .54 is +54%, -1.12 is -112%)
                '24h_vol_price_change_in_btc': calc_price_change(btc_base_agg_result['open'], btc_base_agg_result['close'])
                    if xcp_base_agg_result else None,
                '7d_history_in_xcp': history_in_xcp,
                '7d_history_in_btc': history_in_btc,
            }
        return asset_data

    @dispatcher.add_method
    def get_market_price_history(asset1, asset2, start_ts=None, end_ts=None, as_dict=False):
        """Return block-by-block aggregated market history data for the specified asset pair, within the specified date range.
        @returns List of lists (or list of dicts, if as_dict is specified).
            * If as_dict is False, each embedded list has 8 elements [block time (epoch in MS), open, high, low, close, volume, # trades in block, block index]
            * If as_dict is True, each dict in the list has the keys: block_time (epoch in MS), block_index, open, high, low, close, vol, count 
        """
        if not end_ts: #default to current datetime
            end_ts = time.mktime(datetime.datetime.utcnow().timetuple())
        if not start_ts: #default to 30 days before the end date
            start_ts = end_ts - (30 * 24 * 60 * 60) 
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        
        #get ticks -- open, high, low, close, volume
        result = mongo_db.trades.aggregate([
            {"$match": {
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "block_time": {
                    "$gte": datetime.datetime.fromtimestamp(start_ts),
                    "$lte": datetime.datetime.fromtimestamp(end_ts)
                }
            }},
            {"$project": {
                "block_time": 1,
                "block_index": 1,
                "unit_price": 1,
                "base_amount_normalized": 1 #to derive volume
            }},
            {"$group": {
                "_id":   {"block_time": "$block_time", "block_index": "$block_index"},
                "open":  {"$first": "$unit_price"},
                "high":  {"$max": "$unit_price"},
                "low":   {"$min": "$unit_price"},
                "close": {"$last": "$unit_price"},
                "vol":   {"$sum": "$base_amount_normalized"},
                "count": {"$sum": 1},
            }},
            {"$sort": {"_id.block_time": pymongo.ASCENDING}},
        ])
        if not result['ok']:
            return None
        if as_dict:
            result = result['result']
            for r in result:
                r['block_time'] = r['_id']['block_time']
                r['block_index'] = r['_id']['block_index']
                del r['_id']
        else:
            result = [
                [r['_id']['block_time'],
                 r['open'], r['high'], r['low'], r['close'], r['vol'],
                 r['count'], r['_id']['block_index']] for r in result['result']
            ]
        return result
    
    @dispatcher.add_method
    def get_trade_history(asset1, asset2, last_trades=50):
        """Gets last N of trades for a specified asset pair"""
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        if last_trades > 500:
            raise Exception("Requesting history of too many trades")
        
        last_trades = mongo_db.trades.find({
            "base_asset": base_asset,
            "quote_asset": quote_asset}, {'_id': 0}).sort("block_time", pymongo.DESCENDING).limit(last_trades)
        if not last_trades.count():
            return False #no suitable trade data to form a market price
        last_trades = list(last_trades)
        return last_trades 
    
    @dispatcher.add_method
    def get_trade_history_within_dates(asset1, asset2, start_ts=None, end_ts=None, limit=50):
        """Gets trades for a certain asset pair between a certain date range, with the max results limited"""
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        if not end_ts: #default to current datetime
            end_ts = time.mktime(datetime.datetime.utcnow().timetuple())
        if not start_ts: #default to 30 days before the end date
            start_ts = end_ts - (30 * 24 * 60 * 60) 

        if limit > 500:
            raise Exception("Requesting history of too many trades")
        
        last_trades = mongo_db.trades.find({
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "block_time": {
                    "$gte": datetime.datetime.fromtimestamp(start_ts),
                    "$lte": datetime.datetime.fromtimestamp(end_ts)
                  }
            }, {'_id': 0}).sort("block_time", pymongo.DESCENDING).limit(limit)
        if not last_trades.count():
            return False #no suitable trade data to form a market price
        last_trades = list(last_trades)
        return last_trades 

    @dispatcher.add_method
    def get_order_book(asset1, asset2):
        """Gets the current order book for a specified asset pair"""
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        base_asset_info = mongo_db.tracked_assets.find_one({'asset': base_asset})
        quote_asset_info = mongo_db.tracked_assets.find_one({'asset': quote_asset})
        
        if not base_asset_info or not quote_asset_info:
            raise Exception("Invalid asset(s)")
        
        #TODO: limit # results to 8 or so for each book (we have to sort as well to limit)
        base_bid_orders = util.call_jsonrpc_api("get_orders", {
            'filters': [
                {"field": "get_asset", "op": "==", "value": base_asset},
                {"field": "give_asset", "op": "==", "value": quote_asset},
                {'field': 'give_remaining', 'op': '!=', 'value': 0}, #don't show empty
            ],
            'show_expired': False}, abort_on_error=True)['result']
        base_ask_orders = util.call_jsonrpc_api("get_orders", {
            'filters': [
                {"field": "get_asset", "op": "==", "value": quote_asset},
                {"field": "give_asset", "op": "==", "value": base_asset},
                {'field': 'give_remaining', 'op': '!=', 'value': 0}, #don't show empty
            ],
            'show_expired': False}, abort_on_error=True)['result']
        
        def make_book(orders, isBidBook):
            book = {}
            for o in orders:
                if o['give_asset'] == base_asset:
                    give_amount = util.normalize_amount(o['give_amount'], base_asset_info['divisible'])
                    get_amount = util.normalize_amount(o['get_amount'], quote_asset_info['divisible'])
                    unit_price = float(( D(o['get_amount']) / D(o['give_amount']) ).quantize(
                        D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
                    remaining = util.normalize_amount(o['give_remaining'], base_asset_info['divisible'])
                else:
                    give_amount = util.normalize_amount(o['give_amount'], quote_asset_info['divisible'])
                    get_amount = util.normalize_amount(o['get_amount'], base_asset_info['divisible'])
                    unit_price = float(( D(o['give_amount']) / D(o['get_amount']) ).quantize(
                        D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
                    remaining = util.normalize_amount(o['get_remaining'], base_asset_info['divisible'])
                id = "%s_%s_%s" % (base_asset, quote_asset, unit_price)
                #^ key = {base}_{bid}_{unit_price}, values ref entries in book
                book.setdefault(id, {'unit_price': unit_price, 'amount': 0, 'count': 0})
                book[id]['amount'] += remaining #base amount outstanding
                book[id]['count'] += 1 #num orders at this price level
            book = sorted(book.itervalues(), key=operator.itemgetter('unit_price'), reverse=isBidBook)
            #^ sort -- bid book = descending, ask book = ascending
            return book
        
        #compile into a single book, at volume tiers
        base_bid_book = make_book(base_bid_orders, True)
        base_ask_book = make_book(base_ask_orders, False)
        #get stats like the spread and median
        if base_bid_book and base_ask_book:
            bid_ask_spread = float(( D(base_ask_book[0]['unit_price']) - D(base_bid_book[0]['unit_price']) ).quantize(
                            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        else: bid_ask_spread = 0
        if base_ask_book:
            bid_ask_median = float(( D(base_ask_book[0]['unit_price']) - (D(bid_ask_spread) / 2) ).quantize(
                            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        else: bid_ask_median = 0
        
        #compose depth
        bid_depth = D(0)
        for o in base_bid_book:
            bid_depth += D(o['amount'])
            o['depth'] = float(bid_depth.quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        bid_depth = float(bid_depth.quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        ask_depth = D(0)
        for o in base_ask_book:
            ask_depth += D(o['amount'])
            o['depth'] = float(ask_depth.quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        ask_depth = float(ask_depth.quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        #
        return {
            'base_bid_book': base_bid_book,
            'base_ask_book': base_ask_book,
            'bid_depth': bid_depth,
            'ask_depth': ask_depth,
            'bid_ask_spread': bid_ask_spread,
            'bid_ask_median': bid_ask_median,
        }
    
    @dispatcher.add_method
    def get_owned_assets(addresses):
        """Gets a list of owned assets for one or more addresses"""
        result = mongo_db.tracked_assets.find({
            'owner': {"$in": addresses}
        }, {"_id":0}).sort("asset", pymongo.ASCENDING)
        return list(result)

    @dispatcher.add_method
    def get_balance_history(asset, addresses, normalize=True, start_ts=None, end_ts=None):
        """Retrieves the ordered balance history for a given address (or list of addresses) and asset pair, within the specified date range
        @param normalize: If set to True, return amounts that (if the asset is divisible) have been divided by 100M (satoshi). 
        @return: A list of tuples, with the first entry of each tuple being the block time (epoch TS), and the second being the new balance
         at that block time.
        """
        if not isinstance(addresses, list):
            addresses = [addresses,]
            
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
                    "$gte": datetime.datetime.fromtimestamp(start_ts),
                    "$lte": datetime.datetime.fromtimestamp(end_ts)
                }
            }).sort("block_time", pymongo.ASCENDING)
            results.append({
                'name': address,
                'data': [
                    (r['block_time']*1000,
                     r['new_balance_normalized'] if normalize else r['new_balance']
                    ) for r in result]
            })
        return results

    @dispatcher.add_method
    def get_chat_handle(wallet_id):
        result = mongo_db.chat_handles.find_one({"wallet_id": wallet_id})
        if not result: return {}
        result['last_touched'] = time.mktime(time.gmtime())
        mongo_db.chat_handles.save(result)
        return {
            'handle': result['handle'],
            'last_updated': result.get('last_updated', None)
            } if result else {}

    @dispatcher.add_method
    def store_chat_handle(wallet_id, handle):
        """Set or update a chat handle"""
        if not isinstance(handle, basestring):
            raise Exception("Invalid chat handle: bad data type")
        if not re.match(r'[A-Za-z0-9_-]{4,12}', handle):
            raise Exception("Invalid chat handle: bad syntax/length")

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
    def get_preferences(wallet_id):
        result =  mongo_db.preferences.find_one({"wallet_id": wallet_id})
        if not result: {}
        result['last_touched'] = time.mktime(time.gmtime())
        mongo_db.preferences.save(result)
        return {
            'preferences': json.loads(result['preferences']),
            'last_updated': result.get('last_updated', None)
            } if result else {'preferences': {}, 'last_updated': None}

    @dispatcher.add_method
    def store_preferences(wallet_id, preferences):
        if not isinstance(preferences, dict):
            raise Exception("Invalid preferences object")
        try:
            preferences_json = json.dumps(preferences)
        except:
            raise Exception("Cannot dump preferences to JSON")
        
        #sanity check around max size
        if len(preferences_json) >= PREFERENCES_MAX_LENGTH:
            raise Exception("Preferences object is too big.")
        
        mongo_db.preferences.update(
            {'wallet_id': wallet_id},
            {"$set": {
                'wallet_id': wallet_id,
                'preferences': preferences_json,
                'last_updated': time.mktime(time.gmtime()),
                'last_touched': time.mktime(time.gmtime())
                }
            }, upsert=True)
        #^ last_updated MUST be in GMT, as it will be compaired again other servers
        return True
    
    @dispatcher.add_method
    def proxy_to_counterpartyd(method='', params=[]):
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
            errorMsg = result['error']['data'].get('message', result['error']['message'])
            raise Exception(errorMsg.encode('ascii','ignore'))
            #decode out unicode for now (json-rpc lib was made for python 3.3 and does str(errorMessage) internally,
            # which messes up w/ unicode under python 2.x)
        return result['result']


    class Root(object):
        @cherrypy.expose
        def index(self):
            return ''

    class API(object):
        @cherrypy.expose
        def index(self):
            cherrypy.response.headers["Content-Type"] = 'application/json' 
            cherrypy.response.headers["Access-Control-Allow-Origin"] = '*'
            cherrypy.response.headers["Access-Control-Allow-Methods"] = 'POST, GET, OPTIONS'
            cherrypy.response.headers["Access-Control-Allow-Headers"] = 'Origin, X-Requested-With, Content-Type, Accept'

            if cherrypy.request.method == "OPTIONS": #web client will send us this before making a request
                return
            
            #don't do jack if we're not caught up
            if not config.CAUGHT_UP:
                raise cherrypy.HTTPError(525, 'Server is not caught up. Please try again later.')
                #^ 525 is a custom response code we use for this one purpose
            try:
                data = cherrypy.request.body.read().decode('utf-8')
            except ValueError:
                raise cherrypy.HTTPError(400, 'Invalid JSON document')
            response = JSONRPCResponseManager.handle(data, dispatcher)
            return json.dumps(response.data, default=util.json_dthandler).encode()
    
    cherrypy.config.update({
        'log.screen': False,
        "environment": "embedded",
        'log.error_log.propagate': False,
        'log.access_log.propagate': False,
        "server.logToScreen" : False
    })
    rootApplication = cherrypy.Application(Root(), script_name="")
    apiApplication = cherrypy.Application(API(), script_name="/api")
    cherrypy.tree.mount(rootApplication, '/',
        {'/': { 'tools.trailing_slash.on': False,
                'request.dispatch': cherrypy.dispatch.Dispatcher()}})    
    cherrypy.tree.mount(apiApplication, '/api/',
        {'/': { 'tools.trailing_slash.on': False,
                'request.dispatch': cherrypy.dispatch.Dispatcher()}})    
    
    #disable logging of the access and error logs to the screen
    rootApplication.log.access_log.propagate = False
    rootApplication.log.error_log.propagate = False
    apiApplication.log.access_log.propagate = False
    apiApplication.log.error_log.propagate = False
        
    #set up a rotating log handler for this application
    # Remove the default FileHandlers if present.
    apiApplication.log.error_file = ""
    apiApplication.log.access_file = ""
    maxBytes = getattr(apiApplication.log, "rot_maxBytes", 10000000)
    backupCount = getattr(apiApplication.log, "rot_backupCount", 1000)
    # Make a new RotatingFileHandler for the error log.
    fname = getattr(apiApplication.log, "rot_error_file", os.path.join(config.data_dir, "api.error.log"))
    h = logging_handlers.RotatingFileHandler(fname, 'a', maxBytes, backupCount)
    h.setLevel(logging.DEBUG)
    h.setFormatter(cherrypy._cplogging.logfmt)
    apiApplication.log.error_log.addHandler(h)
    # Make a new RotatingFileHandler for the access log.
    fname = getattr(apiApplication.log, "rot_access_file", os.path.join(config.data_dir, "api.access.log"))
    h = logging_handlers.RotatingFileHandler(fname, 'a', maxBytes, backupCount)
    h.setLevel(logging.DEBUG)
    h.setFormatter(cherrypy._cplogging.logfmt)
    apiApplication.log.access_log.addHandler(h)
    
    #start up the API listener/handler
    server = wsgi.WSGIServer(
        (config.RPC_HOST, int(config.RPC_PORT)), cherrypy.tree)
    server.serve_forever()
