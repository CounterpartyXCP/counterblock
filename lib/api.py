import os
import json
import re
import time
import datetime
import base64
import decimal
import operator
import logging
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
    decimal.getcontext().prec = 8
    
    @dispatcher.add_method
    def is_ready():
        """this method used by the client to check if the server is alive, caught up, and ready to accept requests.
        If the server is NOT caught up, a 525 error will be returned actually before hitting this point. Thus,
        if we actually return data from this function, it should always be true. (may change this behaviour later)"""
        assert config.CAUGHT_UP
        return config.CAUGHT_UP

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
    def get_market_price(asset1, asset2):
        """Gets a synthesized trading "market price" for a specified asset pair (if available).
        If no price is available, False is returned."""
        #look for the last max 6 trades within the past 10 day window
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        min_trade_time = int(time.mktime((datetime.datetime.utcnow() - datetime.timedelta(days=10)).timetuple()))
        last_trades = mongo_db.trades.find({
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            'block_time': {"$gte": min_trade_time}}, {'unit_price': 1}).sort("block_time", pymongo.DESCENDING).limit(6)
        if not last_trades.count():
            return False #no suitable trade data to form a market price
        last_trades = list(last_trades)
        last_trades.reverse() #from newest to oldest
        weights = [1, .9, .72, .6, .4, .3] #good first guess...maybe
        weighted_inputs = []
        for i in xrange(len(last_trades)):
            weighted_inputs.append([last_trades[i]['unit_price'], weights[i]])
        market_price = util.weighted_average(weighted_inputs)
        return float(decimal.Decimal(market_price).quantize(decimal.Decimal('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        #return {
        #    'market_price': float(decimal.Decimal(market_price).quantize(decimal.Decimal('.00000000'), rounding=decimal.ROUND_HALF_EVEN)),
        #    'last_history': [[t['block_time'], t['unit_price'], t['base_amount_normalized'], t['quote_amount_normalized'], t['block_index']] for t in last_trades[:30]]
        #}

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
                "block_time": {"$gte": start_ts, "$lte": end_ts} }},
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
                r['block_time'] = r['_id']['block_time'] * 1000
                r['block_index'] = r['_id']['block_index']
                del r['_id']
        else:
            result = [
                [r['_id']['block_time']*1000, r['open'], r['high'], r['low'], r['close'], r['vol'],
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
    def get_order_book(asset1, asset2):
        """Gets the current order book for a specified asset pair"""
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        base_asset_info = mongo_db.tracked_assets.find_one({'asset': base_asset})
        quote_asset_info = mongo_db.tracked_assets.find_one({'asset': quote_asset})
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
        print "base_bid_book", base_bid_book
        print "base_ask_book", base_ask_book
        #get the bid-ask spread
        bid_ask_spread = float(( D(base_ask_book[0]['unit_price']) - D(base_bid_book[0]['unit_price']) ).quantize(
                        D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        bid_ask_median = float(( D(base_ask_book[0]['unit_price']) - (D(bid_ask_spread) / 2) ).quantize(
                        D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        print "bid_ask_spread", bid_ask_spread, bid_ask_median
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
        if isinstance(addresses, str):
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
                "block_time": {"$gte": start_ts, "$lte": end_ts}
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
            return response.json.encode()
    
    cherrypy.config.update({
        'log.screen': False,
        "environment": "embedded",
        'log.error_log.propagate': False,
        'log.access_log.propagate': False,
        "server.logToScreen" : False
    })
    rootApplication = cherrypy.Application(Root(), script_name="/")
    apiApplication = cherrypy.Application(API(), script_name="/jsonrpc/")
    cherrypy.tree.mount(rootApplication, '/',
        {'/': { 'tools.trailing_slash.on': False,
                'request.dispatch': cherrypy.dispatch.Dispatcher()}})    
    cherrypy.tree.mount(apiApplication, '/jsonrpc/',
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
