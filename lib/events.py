import logging
import datetime
import time
import copy
import decimal

import pymongo
import gevent

from lib import (config, util)

D = decimal.Decimal

def expire_stale_prefs(mongo_db):
    """
    Every day, clear out preferences objects that haven't been touched in > 30 days, in order to reduce abuse risk/space consumed
    """
    min_last_updated = time.mktime((datetime.datetime.utcnow() - datetime.timedelta(days=30)).timetuple())
    
    num_stale_prefs = mongo_db.preferences.find({'last_touched': {'$lte': min_last_updated}}).count()
    mongo_db.preferences.remove({'last_touched': {'$lte': min_last_updated}})
    logging.warn("REMOVED %i stale preferences objects" % num_stale_prefs)
    
    #call again in 1 day
    gevent.spawn_later(86400, expire_stale_prefs, mongo_db)


def compile_asset_market_info(mongo_db):
    """
    Every 10 minutes, run through all assets and compose and store market ranking information.
    This event handler is only run for the first time once we are caught up
    """
    def calc_inverse(quantity):
        return float( (D(1) / D(quantity) ).quantize(
            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))            

    def calc_price_change(open, close):
        return float((D(100) * (D(close) - D(open)) / D(open)).quantize(
                D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))            
    
    def get_price_primatives(start_dt=None, end_dt=None):
        mps_xcp_btc = util.get_market_price_summary(mongo_db, 'XCP', 'BTC', start_dt=start_dt, end_dt=end_dt)
        xcp_btc_price = mps_xcp_btc['market_price'] if mps_xcp_btc else None # == XCP/BTC
        btc_xcp_price = calc_inverse(mps_xcp_btc['market_price']) if mps_xcp_btc else None #BTC/XCP
        return mps_xcp_btc, xcp_btc_price, btc_xcp_price
    
    def get_asset_info(asset, at_dt=None):
        asset_info = mongo_db.tracked_assets.find_one({'asset': asset})
        
        if asset not in ('XCP', 'BTC') and at_dt and asset_info['_at_block_time'] > at_dt:
            #get the asset info at or before the given at_dt datetime
            for e in reversed(asset_info['_history']): #newest to oldest
                if e['_at_block_time'] <= at_dt:
                    asset_info = e
                    break
            else: #asset was created AFTER at_dt
                asset_info = None
            if asset_info is None: return None
            assert asset_info['_at_block_time'] <= at_dt
          
        #modify some of the properties of the returned asset_info for BTC and XCP
        if asset == 'BTC':
            if at_dt:
                start_block_index, end_block_index = util.get_block_indexes_for_dates(mongo_db, end_dt=at_dt)
                asset_info['total_issued'] = util.get_btc_supply(normalize=False, at_block_index=end_block_index)
                asset_info['total_issued_normalized'] = util.normalize_quantity(asset_info['total_issued'])
            else:
                asset_info['total_issued'] = util.get_btc_supply(normalize=False)
                asset_info['total_issued_normalized'] = util.normalize_quantity(asset_info['total_issued'])
        elif asset == 'XCP':
            #BUG: this does not take end_dt (if specified) into account. however, the deviation won't be too big
            # as XCP doesn't deflate quickly at all, and shouldn't matter that much since there weren't any/much trades
            # before the end of the burn period (which is what is involved with how we use at_dt with currently)
            asset_info['total_issued'] = util.call_jsonrpc_api("get_xcp_supply", [], abort_on_error=True)['result']
            asset_info['total_issued_normalized'] = util.normalize_quantity(asset_info['total_issued'])
        if not asset_info:
            raise Exception("Invalid asset: %s" % asset)
        return asset_info
    
    def get_xcp_btc_price_info(asset, mps_xcp_btc, xcp_btc_price, btc_xcp_price, with_last_trades=0, start_dt=None, end_dt=None):
        if asset not in ['BTC', 'XCP']:
            #get price data for both the asset with XCP, as well as BTC
            price_summary_in_xcp = util.get_market_price_summary(mongo_db, asset, 'XCP',
                with_last_trades=with_last_trades, start_dt=start_dt, end_dt=end_dt)
            price_summary_in_btc = util.get_market_price_summary(mongo_db, asset, 'BTC',
                with_last_trades=with_last_trades, start_dt=start_dt, end_dt=end_dt)

            #aggregated (averaged) price (expressed as XCP) for the asset on both the XCP and BTC markets
            if price_summary_in_xcp: # no trade data
                price_in_xcp = price_summary_in_xcp['market_price']
                if xcp_btc_price:
                    aggregated_price_in_xcp = float(((D(price_summary_in_xcp['market_price']) + D(xcp_btc_price)) / D(2)).quantize(
                        D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
                else: aggregated_price_in_xcp = None
            else:
                price_in_xcp = None
                aggregated_price_in_xcp = None
                
            if price_summary_in_btc: # no trade data
                price_in_btc = price_summary_in_btc['market_price']
                if btc_xcp_price:
                    aggregated_price_in_btc = float(((D(price_summary_in_btc['market_price']) + D(btc_xcp_price)) / D(2)).quantize(
                        D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
                else: aggregated_price_in_btc = None
            else:
                aggregated_price_in_btc = None
                price_in_btc = None
        else:
            #here we take the normal XCP/BTC pair, and invert it to BTC/XCP, to get XCP's data in terms of a BTC base
            # (this is the only area we do this, as BTC/XCP is NOT standard pair ordering)
            price_summary_in_xcp = mps_xcp_btc #might be None
            price_summary_in_btc = copy.deepcopy(mps_xcp_btc) if mps_xcp_btc else None #must invert this -- might be None
            if price_summary_in_btc:
                price_summary_in_btc['market_price'] = calc_inverse(price_summary_in_btc['market_price'])
                price_summary_in_btc['base_asset'] = 'BTC'
                price_summary_in_btc['quote_asset'] = 'XCP'
                for i in xrange(len(price_summary_in_btc['last_trades'])):
                    #[0]=block_time, [1]=unit_price, [2]=base_quantity_normalized, [3]=quote_quantity_normalized, [4]=block_index
                    price_summary_in_btc['last_trades'][i][1] = calc_inverse(price_summary_in_btc['last_trades'][i][1])
                    price_summary_in_btc['last_trades'][i][2], price_summary_in_btc['last_trades'][i][3] = \
                        price_summary_in_btc['last_trades'][i][3], price_summary_in_btc['last_trades'][i][2] #swap
            if asset == 'XCP':
                price_in_xcp = 1.0
                price_in_btc = price_summary_in_btc['market_price'] if price_summary_in_btc else None
                aggregated_price_in_xcp = 1.0
                aggregated_price_in_btc = btc_xcp_price #might be None
            else:
                assert asset == 'BTC'
                price_in_xcp = price_summary_in_xcp['market_price'] if price_summary_in_xcp else None
                price_in_btc = 1.0
                aggregated_price_in_xcp = xcp_btc_price #might be None
                aggregated_price_in_btc = 1.0
        return (price_summary_in_xcp, price_summary_in_btc, price_in_xcp, price_in_btc, aggregated_price_in_xcp, aggregated_price_in_btc)
        
    def calc_market_cap(asset_info, price_in_xcp, price_in_btc):
        market_cap_in_xcp = float( (D(asset_info['total_issued_normalized']) / D(price_in_xcp)).quantize(
            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN) ) if price_in_xcp else None
        market_cap_in_btc = float( (D(asset_info['total_issued_normalized']) / D(price_in_btc)).quantize(
            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN) ) if price_in_btc else None
        return market_cap_in_xcp, market_cap_in_btc
    
    def compile_market_info(assets):        
        """Returns information related to capitalization, volume, etc for the supplied asset(s)
        
        NOTE: in_btc == base asset is BTC, in_xcp == base asset is XCP
        
        @param assets: A list of one or more assets
        """
        asset_data = {}
        start_dt_1d = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        start_dt_7d = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        mps_xcp_btc, xcp_btc_price, btc_xcp_price = get_price_primatives()
        for asset in assets:
            asset_info = get_asset_info(asset)

            (price_summary_in_xcp, price_summary_in_btc, price_in_xcp, price_in_btc, aggregated_price_in_xcp, aggregated_price_in_btc
            ) = get_xcp_btc_price_info(asset, mps_xcp_btc, xcp_btc_price, btc_xcp_price, with_last_trades=30)
            
            #get XCP and BTC market summarized trades over a 7d period (quantize to hour long slots)
            _7d_history_in_xcp = None # xcp/asset market (or xcp/btc for xcp or btc)
            _7d_history_in_btc = None # btc/asset market (or btc/xcp for xcp or btc)
            if asset not in ['BTC', 'XCP']:
                for a in ['XCP', 'BTC']:
                    _7d_history = mongo_db.trades.aggregate([
                        {"$match": {
                            "base_asset": a,
                            "quote_asset": asset,
                            "block_time": {"$gte": start_dt_7d }
                        }},
                        {"$project": {
                            "year":  {"$year": "$block_time"},
                            "month": {"$month": "$block_time"},
                            "day":   {"$dayOfMonth": "$block_time"},
                            "hour":  {"$hour": "$block_time"},
                            "unit_price": 1,
                            "base_quantity_normalized": 1 #to derive volume
                        }},
                        {"$sort": {"block_time": pymongo.ASCENDING}},
                        {"$group": {
                            "_id":   {"year": "$year", "month": "$month", "day": "$day", "hour": "$hour"},
                            "price": {"$avg": "$unit_price"},
                            "vol":   {"$sum": "$base_quantity_normalized"},
                        }},
                    ])
                    _7d_history = [] if not _7d_history['ok'] else _7d_history['result']
                    if a == 'XCP': _7d_history_in_xcp = _7d_history
                    else: _7d_history_in_btc = _7d_history
            else: #get the XCP/BTC market and invert for BTC/XCP (_7d_history_in_btc)
                _7d_history = mongo_db.trades.aggregate([
                    {"$match": {
                        "base_asset": 'XCP',
                        "quote_asset": 'BTC',
                        "block_time": {"$gte": start_dt_7d }
                    }},
                    {"$project": {
                        "year":  {"$year": "$block_time"},
                        "month": {"$month": "$block_time"},
                        "day":   {"$dayOfMonth": "$block_time"},
                        "hour":  {"$hour": "$block_time"},
                        "unit_price": 1,
                        "base_quantity_normalized": 1 #to derive volume
                    }},
                    {"$sort": {"block_time": pymongo.ASCENDING}},
                    {"$group": {
                        "_id":   {"year": "$year", "month": "$month", "day": "$day", "hour": "$hour"},
                        "price": {"$avg": "$unit_price"},
                        "vol":   {"$sum": "$base_quantity_normalized"},
                    }},
                ])
                _7d_history = [] if not _7d_history['ok'] else _7d_history['result']
                _7d_history_in_xcp = _7d_history
                _7d_history_in_btc = copy.deepcopy(_7d_history_in_xcp)
                for i in xrange(len(_7d_history_in_btc)):
                    _7d_history_in_btc[i]['price'] = calc_inverse(_7d_history_in_btc[i]['price'])
                    _7d_history_in_btc[i]['vol'] = calc_inverse(_7d_history_in_btc[i]['vol'])
            
            for l in [_7d_history_in_xcp, _7d_history_in_btc]:
                for e in l: #convert our _id field out to be an epoch ts (in ms), and delete _id
                    e['when'] = time.mktime(datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day'], e['_id']['hour']).timetuple()) * 1000 
                    del e['_id']

            #perform aggregation to get 24h statistics
            #TOTAL volume and count across all trades for the asset (on ALL markets, not just XCP and BTC pairings)
            _24h_vols = {'vol': 0, 'count': 0}
            _24h_vols_as_base = mongo_db.trades.aggregate([
                {"$match": {
                    "base_asset": asset,
                    "block_time": {"$gte": start_dt_1d } }},
                {"$project": {
                    "base_quantity_normalized": 1 #to derive volume
                }},
                {"$group": {
                    "_id":   1,
                    "vol":   {"$sum": "$base_quantity_normalized"},
                    "count": {"$sum": 1},
                }}
            ])
            _24h_vols_as_base = {} if not _24h_vols_as_base['ok'] \
                or not len(_24h_vols_as_base['result']) else _24h_vols_as_base['result'][0]
            _24h_vols_as_quote = mongo_db.trades.aggregate([
                {"$match": {
                    "quote_asset": asset,
                    "block_time": {"$gte": start_dt_1d } }},
                {"$project": {
                    "quote_quantity_normalized": 1 #to derive volume
                }},
                {"$group": {
                    "_id":   1,
                    "vol":   {"$sum": "quote_quantity_normalized"},
                    "count": {"$sum": 1},
                }}
            ])
            _24h_vols_as_quote = {} if not _24h_vols_as_quote['ok'] \
                or not len(_24h_vols_as_quote['result']) else _24h_vols_as_quote['result'][0]
            _24h_vols['vol'] = _24h_vols_as_base.get('vol', 0) + _24h_vols_as_quote.get('vol', 0) 
            _24h_vols['count'] = _24h_vols_as_base.get('count', 0) + _24h_vols_as_quote.get('count', 0) 
            
            #XCP market volume with stats
            if asset != 'XCP' and price_summary_in_xcp is not None and len(price_summary_in_xcp['last_trades']):
                _24h_ohlc_in_xcp = mongo_db.trades.aggregate([
                    {"$match": {
                        "base_asset": "XCP",
                        "quote_asset": asset,
                        "block_time": {"$gte": start_dt_1d } }},
                    {"$project": {
                        "unit_price": 1,
                        "base_quantity_normalized": 1 #to derive volume
                    }},
                    {"$group": {
                        "_id":   1,
                        "open":  {"$first": "$unit_price"},
                        "high":  {"$max": "$unit_price"},
                        "low":   {"$min": "$unit_price"},
                        "close": {"$last": "$unit_price"},
                        "vol":   {"$sum": "$base_quantity_normalized"},
                        "count": {"$sum": 1},
                    }}
                ])
                _24h_ohlc_in_xcp = {} if not _24h_ohlc_in_xcp['ok'] \
                    or not len(_24h_ohlc_in_xcp['result']) else _24h_ohlc_in_xcp['result'][0]
                if _24h_ohlc_in_xcp: del _24h_ohlc_in_xcp['_id']
            else:
                _24h_ohlc_in_xcp = {}
                
            #BTC market volume with stats
            if asset != 'BTC' and price_summary_in_btc and len(price_summary_in_btc['last_trades']):
                _24h_ohlc_in_btc = mongo_db.trades.aggregate([
                    {"$match": {
                        "base_asset": "BTC",
                        "quote_asset": asset,
                        "block_time": {"$gte": start_dt_1d } }},
                    {"$project": {
                        "unit_price": 1,
                        "base_quantity_normalized": 1 #to derive volume
                    }},
                    {"$group": {
                        "_id":   1,
                        "open":  {"$first": "$unit_price"},
                        "high":  {"$max": "$unit_price"},
                        "low":   {"$min": "$unit_price"},
                        "close": {"$last": "$unit_price"},
                        "vol":   {"$sum": "$base_quantity_normalized"},
                        "count": {"$sum": 1},
                    }}
                ])
                _24h_ohlc_in_btc = {} if not _24h_ohlc_in_btc['ok'] \
                    or not len(_24h_ohlc_in_btc['result']) else _24h_ohlc_in_btc['result'][0]
                if _24h_ohlc_in_btc: del _24h_ohlc_in_btc['_id']
            else:
                _24h_ohlc_in_btc = {}
            
            market_cap_in_xcp, market_cap_in_btc = calc_market_cap(asset_info, price_in_xcp, price_in_btc)
            asset_data[asset] = {
                'asset': asset,
                'price_in_xcp': price_in_xcp, #current price of asset vs XCP (e.g. how many units of asset for 1 unit XCP)
                'price_in_btc': price_in_btc, #current price of asset vs BTC (e.g. how many units of asset for 1 unit BTC)
                'price_as_xcp': calc_inverse(price_in_xcp) if price_in_xcp else None, #current price of asset AS XCP
                'price_as_btc': calc_inverse(price_in_btc) if price_in_btc else None, #current price of asset AS BTC
                'aggregated_price_in_xcp': aggregated_price_in_xcp, 
                'aggregated_price_in_btc': aggregated_price_in_btc,
                'aggregated_price_as_xcp': calc_inverse(aggregated_price_in_xcp) if aggregated_price_in_xcp else None, 
                'aggregated_price_as_btc': calc_inverse(aggregated_price_in_btc) if aggregated_price_in_btc else None,
                'total_supply': asset_info['total_issued_normalized'], 
                'market_cap_in_xcp': market_cap_in_xcp,
                'market_cap_in_btc': market_cap_in_btc,
                '24h_summary': _24h_vols,
                #^ total quantity traded of that asset in all markets in last 24h
                '24h_ohlc_in_xcp': _24h_ohlc_in_xcp,
                #^ quantity of asset traded with BTC in last 24h
                '24h_ohlc_in_btc': _24h_ohlc_in_btc,
                #^ quantity of asset traded with XCP in last 24h
                '24h_vol_price_change_in_xcp': calc_price_change(_24h_ohlc_in_xcp['open'], _24h_ohlc_in_xcp['close'])
                    if _24h_ohlc_in_xcp else None,
                #^ aggregated price change from 24h ago to now, expressed as a signed float (e.g. .54 is +54%, -1.12 is -112%)
                '24h_vol_price_change_in_btc': calc_price_change(_24h_ohlc_in_btc['open'], _24h_ohlc_in_btc['close'])
                    if _24h_ohlc_in_btc else None,
                '7d_history_in_xcp': [[e['when'], e['price']] for e in _7d_history_in_xcp],
                '7d_history_in_btc': [[e['when'], e['price']] for e in _7d_history_in_btc],
            }
        return asset_data
    
    #grab the last block # we processed assets data off of
    last_block_assets_compiled = mongo_db.app_config.find_one()['last_block_assets_compiled']
    last_block_time_assets_compiled = util.get_block_time(mongo_db, last_block_assets_compiled)
    #logging.debug("Comping info for assets traded since block %i" % last_block_assets_compiled)
    current_block_index = config.CURRENT_BLOCK_INDEX #store now as it may change as we are compiling asset data :)
    current_block_time = util.get_block_time(mongo_db, current_block_index)
    
    if current_block_index == last_block_assets_compiled:
        #all caught up -- call again in 10 minutes
        gevent.spawn_later(10 * 60, compile_asset_market_info, mongo_db)
        return
        
    #get assets that were traded since the last check on ANY pair that the asset was involved in (even if the pair did not involve XCP or BTC)
    # this is important because compiled market info has a 24h vol parameter that designates total volume for the asset across ALL pairings
    distinct_base = mongo_db.trades.find({'block_index': {'$gt': last_block_assets_compiled}},
        {'base_asset': 1, '_id': 0}).distinct('base_asset')
    distinct_quote = mongo_db.trades.find({'block_index': {'$gt': last_block_assets_compiled}},
        {'quote_asset': 1, '_id': 0}).distinct('quote_asset')
    traded_assets = list(set(distinct_base + distinct_quote)) #convert to set to uniquify
    #update our storage of the latest market info in mongo
    compiled_assets_info = compile_market_info(traded_assets)
    for asset, asset_info in compiled_assets_info.iteritems():
        logging.info("Block: %s -- Updating asset market info for %s ..." % (current_block_index, asset))
        mongo_db.asset_market_info.update( {'asset': asset}, {"$set": asset_info}, upsert=True)
        
    #next, compile market cap historicals
    #start by getting all trades from when we last compiled this data
    trades = mongo_db.trades.find({'block_index': {'$gt': last_block_assets_compiled}}).sort('block_index', pymongo.ASCENDING)
    trades_block_mapping = {} #tracks assets compiled per block, as we only want to analyze any given asset once per block
    for t in trades:
        #ensure that we only process an asset that hasn't already been processed for this block
        # (as there could be multiple trades in a single block for any specific asset)
        assets = []
        trades_block_mapping.setdefault(t['block_index'], {})
        if t['base_asset'] not in trades_block_mapping[t['block_index']]:
            assets.append(t['base_asset'])
        if t['quote_asset'] not in trades_block_mapping[t['block_index']]:
            assets.append(t['quote_asset'])
        if not len(assets): continue

        mps_xcp_btc, xcp_btc_price, btc_xcp_price = get_price_primatives(end_dt=t['block_time'])        
        for asset in assets:
            #recalculate the market cap for the asset this trade is for
            asset_info = get_asset_info(asset, at_dt=t['block_time'])
            (price_summary_in_xcp, price_summary_in_btc, price_in_xcp, price_in_btc, aggregated_price_in_xcp, aggregated_price_in_btc
            ) = get_xcp_btc_price_info(asset, mps_xcp_btc, xcp_btc_price, btc_xcp_price, with_last_trades=0, end_dt=t['block_time'])
            market_cap_in_xcp, market_cap_in_btc = calc_market_cap(asset_info, price_in_xcp, price_in_btc)
            #^ this will get price data from the block time of this trade back the standard number of days and trades
            # to determine our standard market price, relative (anchored) to the time of this trade
    
            for market_cap_as in ('XCP', 'BTC'):
                market_cap = market_cap_in_xcp if market_cap_as == 'XCP' else market_cap_in_btc
                #if there is a previously stored market cap for this asset, add a new history point only if the two caps differ
                prev_market_cap_history = mongo_db.asset_marketcap_history.find({'market_cap_as': market_cap_as, 'asset': asset,
                    'block_index': {'$lt': t['block_index']}}).sort('block_index', pymongo.DESCENDING).limit(1)
                prev_market_cap_history = list(prev_market_cap_history)[0] if prev_market_cap_history.count() == 1 else None
                
                if market_cap and (not prev_market_cap_history or prev_market_cap_history['market_cap'] != market_cap):
                    mongo_db.asset_marketcap_history.insert({
                        'block_index': t['block_index'],
                        'block_time': t['block_time'],
                        'asset': asset,
                        'market_cap': market_cap,
                        'market_cap_as': market_cap_as,
                    })
                    logging.info("Block %i -- Calculated market cap history point for %s as %s" % (t['block_index'], asset, market_cap_as))
                            
    #call again in 10 minutes
    gevent.spawn_later(10 * 60, compile_asset_market_info, mongo_db)
    
    mongo_db.app_config.update({}, {'$set': {'last_block_assets_compiled': current_block_index}})
    