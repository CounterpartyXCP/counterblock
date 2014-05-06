import os
import re
import logging
import datetime
import time
import copy
import decimal
import json
import StringIO

import grequests
import pymongo
import gevent
from PIL import Image

from lib import (config, util, util_trading)

from betting import Betting

D = decimal.Decimal
COMPILE_MARKET_PAIR_INFO_PERIOD = 10 * 60 #in seconds (this is every 10 minutes currently)
COMPILE_ASSET_MARKET_INFO_PERIOD = 30 * 60 #in seconds (this is every 30 minutes currently)

def check_insight():
    try:
        r = grequests.map((grequests.get(config.INSIGHT + '/api/sync/'),) )[0]
        if (not r or not hasattr(r, 'status_code')):
            raise Exception("Could not contact insight!")
        if r.status_code != 200:
            raise ValueError("Bad status code returned from insight: %s" % r.status_code)
        result = r.json()
        if result['status'] == 'error':
            raise Exception('Insight reports error: %s' % result['error'])
        if result['status'] == 'syncing':
            logging.warning("WARNING: Insight is not fully synced to the blockchain: %s%% complete" % result['syncPercentage'])
    except Exception as e:
        raise Exception('Could not connect to Insight server: %s' % e)
    finally:
        gevent.spawn_later(5 * 60, check_insight) #call again in 5 minutes

def expire_stale_prefs():
    """
    Every day, clear out preferences objects that haven't been touched in > 30 days, in order to reduce abuse risk/space consumed
    """
    mongo_db = config.mongo_db
    min_last_updated = time.mktime((datetime.datetime.utcnow() - datetime.timedelta(days=30)).timetuple())
    
    num_stale_records = config.mongo_db.preferences.find({'last_touched': {'$lt': min_last_updated}}).count()
    mongo_db.preferences.remove({'last_touched': {'$lt': min_last_updated}})
    if num_stale_records: logging.warn("REMOVED %i stale preferences objects" % num_stale_records)
    
    #call again in 1 day
    gevent.spawn_later(86400, expire_stale_prefs)

def expire_stale_btc_open_order_records():
    mongo_db = config.mongo_db
    min_when_created = time.mktime((datetime.datetime.utcnow() - datetime.timedelta(days=15)).timetuple())
    
    num_stale_records = config.mongo_db.btc_open_orders.find({'when_created': {'$lt': min_when_created}}).count()
    mongo_db.btc_open_orders.remove({'when_created': {'$lt': min_when_created}})
    if num_stale_records: logging.warn("REMOVED %i stale BTC open order objects" % num_stale_records)
    
    #call again in 1 day
    gevent.spawn_later(86400, expire_stale_btc_open_order_records)

def compile_asset_pair_market_info():
    """Compiles the pair-level statistics that show on the View Prices page of counterwallet, for instance"""
    #loop through all open orders, and compile a listing of pairs, with a count of open orders for each pair
    mongo_db = config.mongo_db
    end_dt = datetime.datetime.utcnow()
    start_dt = end_dt - datetime.timedelta(days=1)
    start_block_index, end_block_index = util.get_block_indexes_for_dates(start_dt=start_dt, end_dt=end_dt)
    open_orders = util.call_jsonrpc_api("get_orders",
        { 'filters': [
            {'field': 'give_remaining', 'op': '>', 'value': 0},
            {'field': 'get_remaining', 'op': '>', 'value': 0},
            {'field': 'fee_required_remaining', 'op': '>=', 'value': 0},
            {'field': 'fee_provided_remaining', 'op': '>=', 'value': 0},
          ],
          'show_expired': False,
          #'order_by': 'block_index',
          #'order_dir': 'asc'
        }, abort_on_error=True)['result']
    pair_data = {}
    asset_info = {}
    
    def get_price(base_quantity_normalized, quote_quantity_normalized):
        return float(D(quote_quantity_normalized / base_quantity_normalized ).quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
    
    #COMPOSE order depth, lowest ask, and highest bid column data
    for o in open_orders:
        (base_asset, quote_asset) = util.assets_to_asset_pair(o['give_asset'], o['get_asset'])
        pair = '%s/%s' % (base_asset, quote_asset)
        base_asset_info = asset_info.get(base_asset, mongo_db.tracked_assets.find_one({ 'asset': base_asset }))
        if base_asset not in asset_info: asset_info[base_asset] = base_asset_info
        quote_asset_info = asset_info.get(quote_asset, mongo_db.tracked_assets.find_one({ 'asset': quote_asset }))
        if quote_asset not in asset_info: asset_info[quote_asset] = quote_asset_info
        
        pair_data.setdefault(pair, {'open_orders_count': 0, 'lowest_ask': None, 'highest_bid': None,
            'completed_trades_count': 0, 'vol_base': 0, 'vol_quote': 0})
        #^ highest ask = open order selling base, highest bid = open order buying base
        #^ we also initialize completed_trades_count, vol_base, vol_quote because every pair inited here may
        # not have cooresponding data out of the trades_data_by_pair aggregation below
        pair_data[pair]['open_orders_count'] += 1
        base_quantity_normalized = util.normalize_quantity(o['give_quantity'] if base_asset == o['give_asset'] else o['get_quantity'], base_asset_info['divisible'])
        quote_quantity_normalized = util.normalize_quantity(o['give_quantity'] if quote_asset == o['give_asset'] else o['get_quantity'], quote_asset_info['divisible'])
        order_price = get_price(base_quantity_normalized, quote_quantity_normalized)
        if base_asset == o['give_asset']: #selling base
            if pair_data[pair]['lowest_ask'] is None or order_price < pair_data[pair]['lowest_ask']: 
                pair_data[pair]['lowest_ask'] = order_price
        elif base_asset == o['get_asset']: #buying base
            if pair_data[pair]['highest_bid'] is None or order_price > pair_data[pair]['highest_bid']:
                pair_data[pair]['highest_bid'] = order_price
    
    #COMPOSE volume data (in XCP and BTC), and % change data
    #loop through all trade volume over the past 24h, and match that to the open orders
    trades_data_by_pair = mongo_db.trades.aggregate([
        {"$match": {
            "block_time": {"$gte": start_dt, "$lte": end_dt } }
        },
        {"$project": {
            "base_asset": 1,
            "quote_asset": 1,
            "base_quantity_normalized": 1, #to derive base volume
            "quote_quantity_normalized": 1 #to derive quote volume
        }},
        {"$group": {
            "_id":   {"base_asset": "$base_asset", "quote_asset": "$quote_asset"},
            "vol_base":   {"$sum": "$base_quantity_normalized"},
            "vol_quote":   {"$sum": "$quote_quantity_normalized"},
            "count": {"$sum": 1},
        }}
    ])
    trades_data_by_pair = [] if not trades_data_by_pair['ok'] else trades_data_by_pair['result']
    for e in trades_data_by_pair:
        pair = '%s/%s' % (e['_id']['base_asset'], e['_id']['quote_asset'])
        pair_data.setdefault(pair, {'open_orders_count': 0, 'lowest_ask': None, 'highest_bid': None})
        #^ initialize an empty pair in the event there are no open orders for that pair, but there ARE completed trades for it
        pair_data[pair]['completed_trades_count'] = e['count']
        pair_data[pair]['vol_base'] = e['vol_base'] 
        pair_data[pair]['vol_quote'] = e['vol_quote'] 
    
    #compose price data, relative to BTC and XCP
    mps_xcp_btc, xcp_btc_price, btc_xcp_price = util_trading.get_price_primatives()
    for pair, e in pair_data.iteritems():
        base_asset, quote_asset = pair.split('/')
        _24h_vol_in_btc = None
        _24h_vol_in_xcp = None
        #derive asset price data, expressed in BTC and XCP, for the given volumes
        if base_asset == 'XCP':
            _24h_vol_in_xcp = e['vol_base']
            _24h_vol_in_btc = util.round_out(e['vol_base'] * xcp_btc_price)
        elif base_asset == 'BTC':
            _24h_vol_in_xcp = util.round_out(e['vol_base'] * btc_xcp_price)
            _24h_vol_in_btc = e['vol_base']
        else: #base is not XCP or BTC
            price_summary_in_xcp, price_summary_in_btc, price_in_xcp, price_in_btc, aggregated_price_in_xcp, aggregated_price_in_btc = \
                util_trading.get_xcp_btc_price_info(base_asset, mps_xcp_btc, xcp_btc_price, btc_xcp_price, with_last_trades=0, start_dt=start_dt, end_dt=end_dt)
            if price_in_xcp:
                _24h_vol_in_xcp = util.round_out(e['vol_base'] * price_in_xcp)
            if price_in_btc:
                _24h_vol_in_btc = util.round_out(e['vol_base'] * price_in_btc)
            
            if _24h_vol_in_xcp is None or _24h_vol_in_btc is None:
                #the base asset didn't have price data against BTC or XCP, or both...try against the quote asset instead
                price_summary_in_xcp, price_summary_in_btc, price_in_xcp, price_in_btc, aggregated_price_in_xcp, aggregated_price_in_btc = \
                    util_trading.get_xcp_btc_price_info(quote_asset, mps_xcp_btc, xcp_btc_price, btc_xcp_price, with_last_trades=0, start_dt=start_dt, end_dt=end_dt)
                if _24h_vol_in_xcp is None and price_in_xcp:
                    _24h_vol_in_xcp = util.round_out(e['vol_quote'] * price_in_xcp)
                if _24h_vol_in_btc is None and price_in_btc:
                    _24h_vol_in_btc = util.round_out(e['vol_quote'] * price_in_btc)
            pair_data[pair]['24h_vol_in_xcp'] = _24h_vol_in_xcp #might still be None
            pair_data[pair]['24h_vol_in_btc'] = _24h_vol_in_btc #might still be None
        
        #get % change stats -- start by getting the first trade directly before the 24h period starts
        prev_trade = mongo_db.trades.find({
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "block_time": {'$lt': start_dt}}).sort('block_time', pymongo.DESCENDING).limit(1)
        latest_trade = mongo_db.trades.find({
            "base_asset": base_asset,
            "quote_asset": quote_asset}).sort('block_time', pymongo.DESCENDING).limit(1)
        if not prev_trade.count(): #no previous trade before this 24hr period
            pair_data[pair]['24h_pct_change'] = None
        else:
            prev_trade = prev_trade[0]
            latest_trade = latest_trade[0]
            prev_trade_price = get_price(prev_trade['base_quantity_normalized'], prev_trade['quote_quantity_normalized'])
            latest_trade_price = get_price(latest_trade['base_quantity_normalized'], latest_trade['quote_quantity_normalized'])
            pair_data[pair]['24h_pct_change'] = ((latest_trade_price - prev_trade_price) / prev_trade_price) * 100
        pair_data[pair]['last_updated'] = end_dt
        #print "PRODUCED", pair, pair_data[pair] 
        mongo_db.asset_pair_market_info.update( {'base_asset': base_asset, 'quote_asset': quote_asset}, {"$set": pair_data[pair]}, upsert=True)
        
    #remove any old pairs that were not just updated
    mongo_db.asset_pair_market_info.remove({'last_updated': {'$lt': end_dt}})
    
    logging.info("Recomposed 24h trade statistics for %i asset pairs: %s" % (len(pair_data), ', '.join(pair_data.keys())))
    #all done for this run...call again in a bit                            
    gevent.spawn_later(COMPILE_MARKET_PAIR_INFO_PERIOD, compile_asset_pair_market_info)

def compile_extended_asset_info():
    mongo_db = config.mongo_db
    #create directory if it doesn't exist
    imageDir = os.path.join(config.data_dir, config.SUBDIR_ASSET_IMAGES)
    if not os.path.exists(imageDir):
        os.makedirs(imageDir)
        
    assets_info = mongo_db.asset_extended_info.find()
    for asset_info in assets_info:
        if asset_info.get('disabled', False):
            logging.info("ExtendedAssetInfo: Skipping disabled asset %s" % asset_info['asset'])
            continue
        
        #try to get the data at the specified URL
        assert 'url' in asset_info and util.is_valid_url(asset_info['url'], suffix='.json')
        data = {}
        raw_image_data = None
        try:
            #TODO: Right now this loop makes one request at a time. Fully utilize grequests to make batch requests
            # at the same time (using map() and throttling) 
            r = grequests.map((grequests.get(asset_info['url'], timeout=1, stream=True),), stream=True)[0]
            if not r: raise Exception("Invalid response")
            if r.status_code != 200: raise Exception("Got non-successful response code of: %s" % r.status_code)
            #read up to 4KB and try to convert to JSON
            raw_data = r.raw.read(4 * 1024, decode_content=True)
            r.raw.release_conn()
            data = json.loads(raw_data)
            #if here, we have valid json data
            if 'asset' not in data:
                raise Exception("Missing asset field")
            if 'description' not in data:
                data['description'] = ''
            if 'image' not in data:
                data['image'] = ''
            if 'website' not in data:
                data['website'] = ''
            if 'pgpsig' not in data:
                data['pgpsig'] = ''
                
            if data['asset'] != asset_info['asset']:
                raise Exception("asset field is invalid (is: '%s', should be: '%s')" % (data['asset'], asset_info['asset']))
            if data['image'] and (not util.is_valid_url(data['image'] or len(data['website']) > 100)):
                raise Exception("'image' field is not valid URL, or over the max allowed length")
            if data['website'] and (not util.is_valid_url(data['website'] or len(data['website']) > 100)):
                raise Exception("'website' field is not valid URL, or over the max allowed length")
            if data['pgpsig'] and (not util.is_valid_url(data['pgpsig'] or len(data['pgpsig']) > 100)):
                raise Exception("'pgpsig' field is not valid URL, or over the max allowed length")
            
            if data['image']:
                #fetch the image data (must be 32x32 png, max 20KB size)
                r = grequests.map((grequests.get(data['image'], timeout=1, stream=True),), stream=True)[0]
                if not r: raise Exception("Invalid response")
                if r.status_code != 200: raise Exception("Got non-successful response code of: %s" % r.status_code)
                #read up to 20KB and try to convert to JSON
                raw_image_data = r.raw.read(20 * 1024, decode_content=True)
                r.raw.release_conn()
                try:
                    image = Image.open(StringIO.StringIO(raw_image_data))
                except:
                    raise Exception("Unable to parse image data at: %s" % data['image'])
                if image.format != 'PNG': raise Exception("Image is not a PNG: %s (got %s)" % (data['image'], image.format))
                if image.size != (48, 48): raise Exception("Image size is not 48x48: %s (got %s)" % (data['image'], image.size))
                if image.mode not in ['RGB', 'RGBA']: raise Exception("Image mode is not RGB/RGBA: %s (got %s)" % (data['image'], image.mode))
        except Exception, e:
            logging.info("ExtendedAssetInfo: Skipped asset %s: %s" % (asset_info['asset'], e))
        else:
            asset_info['description'] = util.sanitize_eliteness(data['description'])
            asset_info['website'] = util.sanitize_eliteness(data['website']) #just in case (paranoid)
            asset_info['pgpsig'] = util.sanitize_eliteness(data['pgpsig']) #just in case (paranoid)
            asset_info['image'] = util.sanitize_eliteness(data['image']) #just in case (paranoid)
            if data['image'] and raw_image_data:
                #save the image to disk
                imagePath = os.path.join(imageDir, data['asset'] + '.png')
                image.save(imagePath)
                os.system("exiftool -q -overwrite_original -all= %s" % imagePath) #strip all metadata, just in case
            mongo_db.asset_extended_info.save(asset_info)
            logging.info("ExtendedAssetInfo: Compiled data for asset %s" % asset_info['asset'])
        
    #call again in 60 minutes
    gevent.spawn_later(60 * 60, compile_extended_asset_info)

def compile_extended_feed_info():
    betting = Betting(config.mongo_db)
    betting.fetch_all_feed_info()
    #call again in 5 minutes
    gevent.spawn_later(60 * 1, compile_extended_feed_info)

def compile_asset_market_info():
    """
    Every 10 minutes, run through all assets and compose and store market ranking information.
    This event handler is only run for the first time once we are caught up
    """
    if not config.CAUGHT_UP:
        logging.warn("Not updating asset market info as CAUGHT_UP is false.")
        gevent.spawn_later(COMPILE_ASSET_MARKET_INFO_PERIOD, compile_asset_market_info)
        return
    
    mongo_db = config.mongo_db
    #grab the last block # we processed assets data off of
    last_block_assets_compiled = mongo_db.app_config.find_one()['last_block_assets_compiled']
    last_block_time_assets_compiled = util.get_block_time(last_block_assets_compiled)
    #logging.debug("Comping info for assets traded since block %i" % last_block_assets_compiled)
    current_block_index = config.CURRENT_BLOCK_INDEX #store now as it may change as we are compiling asset data :)
    current_block_time = util.get_block_time(current_block_index)

    if current_block_index == last_block_assets_compiled:
        #all caught up -- call again in 10 minutes
        gevent.spawn_later(COMPILE_ASSET_MARKET_INFO_PERIOD, compile_asset_market_info)
        return

    mps_xcp_btc, xcp_btc_price, btc_xcp_price = util_trading.get_price_primatives()
    all_traded_assets = list(set(list(['BTC', 'XCP']) + list(mongo_db.trades.find({}, {'quote_asset': 1, '_id': 0}).distinct('quote_asset'))))
    
    #######################
    #get a list of all assets with a trade within the last 24h (not necessarily just against XCP and BTC)
    # ^ this is important because compiled market info has a 24h vol parameter that designates total volume for the asset across ALL pairings
    start_dt_1d = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    
    assets = list(set(
          list(mongo_db.trades.find({'block_time': {'$gte': start_dt_1d}}).distinct('quote_asset'))
        + list(mongo_db.trades.find({'block_time': {'$gte': start_dt_1d}}).distinct('base_asset'))
    ))
    for asset in assets:
        market_info_24h = util_trading.compile_24h_market_info(asset)
        mongo_db.asset_market_info.update({'asset': asset}, {"$set": market_info_24h})
    #for all others (i.e. no trade in the last 24 hours), zero out the 24h trade data
    non_traded_assets = list(set(all_traded_assets) - set(assets))
    mongo_db.asset_market_info.update( {'asset': {'$in': non_traded_assets}}, {"$set": {
            '24h_summary': {'vol': 0, 'count': 0},
            '24h_ohlc_in_xcp': {},
            '24h_ohlc_in_btc': {},
            '24h_vol_price_change_in_xcp': None,
            '24h_vol_price_change_in_btc': None,
    }}, multi=True)
    logging.info("Block: %s -- Calculated 24h stats for: %s" % (current_block_index, ', '.join(assets)))
    
    #######################
    #get a list of all assets with a trade within the last 7d up against XCP and BTC
    start_dt_7d = datetime.datetime.utcnow() - datetime.timedelta(days=7)
    assets = list(set(
          list(mongo_db.trades.find({'block_time': {'$gte': start_dt_7d}, 'base_asset': {'$in': ['XCP', 'BTC']}}).distinct('quote_asset'))
        + list(mongo_db.trades.find({'block_time': {'$gte': start_dt_7d}}).distinct('base_asset'))
    ))
    for asset in assets:
        market_info_7d = util_trading.compile_7d_market_info(asset)
        mongo_db.asset_market_info.update({'asset': asset}, {"$set": market_info_7d})
    non_traded_assets = list(set(all_traded_assets) - set(assets))
    mongo_db.asset_market_info.update( {'asset': {'$in': non_traded_assets}}, {"$set": {
            '7d_history_in_xcp': [],
            '7d_history_in_btc': [],
    }}, multi=True)
    logging.info("Block: %s -- Calculated 7d stats for: %s" % (current_block_index, ', '.join(assets)))

    #######################
    #update summary market data for assets traded since last_block_assets_compiled
    #get assets that were traded since the last check with either BTC or XCP, and update their market summary data
    assets = list(set(
          list(mongo_db.trades.find({'block_index': {'$gt': last_block_assets_compiled}, 'base_asset': {'$in': ['XCP', 'BTC']}}).distinct('quote_asset'))
        + list(mongo_db.trades.find({'block_index': {'$gt': last_block_assets_compiled}}).distinct('base_asset'))
    ))
    #update our storage of the latest market info in mongo
    for asset in assets:
        logging.info("Block: %s -- Updating asset market info for %s ..." % (current_block_index, asset))
        summary_info = util_trading.compile_summary_market_info(asset, mps_xcp_btc, xcp_btc_price, btc_xcp_price)
        mongo_db.asset_market_info.update( {'asset': asset}, {"$set": summary_info}, upsert=True)

    
    #######################
    #next, compile market cap historicals (and get the market price data that we can use to update assets with new trades)
    #NOTE: this algoritm still needs to be fleshed out some...I'm not convinced it's laid out/optimized like it should be
    #start by getting all trades from when we last compiled this data
    trades = mongo_db.trades.find({'block_index': {'$gt': last_block_assets_compiled}}).sort('block_index', pymongo.ASCENDING)
    trades_by_block = [] #tracks assets compiled per block, as we only want to analyze any given asset once per block
    trades_by_block_mapping = {} 
    #organize trades by block
    for t in trades:
        if t['block_index'] in trades_by_block_mapping:
            assert trades_by_block_mapping[t['block_index']]['block_index'] == t['block_index']
            assert trades_by_block_mapping[t['block_index']]['block_time'] == t['block_time']
            trades_by_block_mapping[t['block_index']]['trades'].append(t)
        else:
            e = {'block_index': t['block_index'], 'block_time': t['block_time'], 'trades': [t,]}
            trades_by_block.append(e)
            trades_by_block_mapping[t['block_index']] = e  

    for t_block in trades_by_block:
        #reverse the tradelist per block, and ensure that we only process an asset that hasn't already been processed for this block
        # (as there could be multiple trades in a single block for any specific asset). we reverse the list because
        # we'd rather process a later trade for a given asset, as the market price for that will take into account
        # the earlier trades on that same block for that asset, and we don't want/need multiple cap points per block
        assets_in_block = {}
        mps_xcp_btc, xcp_btc_price, btc_xcp_price = util_trading.get_price_primatives(end_dt=t_block['block_time'])
        for t in reversed(t_block['trades']):
            assets = []
            if t['base_asset'] not in assets_in_block:
                assets.append(t['base_asset'])
                assets_in_block[t['base_asset']] = True
            if t['quote_asset'] not in assets_in_block:
                assets.append(t['quote_asset'])
                assets_in_block[t['quote_asset']] = True
            if not len(assets): continue
    
            for asset in assets:
                #recalculate the market cap for the asset this trade is for
                asset_info = util_trading.get_asset_info(asset, at_dt=t['block_time'])
                (price_summary_in_xcp, price_summary_in_btc, price_in_xcp, price_in_btc, aggregated_price_in_xcp, aggregated_price_in_btc
                ) = util_trading.get_xcp_btc_price_info(asset, mps_xcp_btc, xcp_btc_price, btc_xcp_price, with_last_trades=0, end_dt=t['block_time'])
                market_cap_in_xcp, market_cap_in_btc = util_trading.calc_market_cap(asset_info, price_in_xcp, price_in_btc)
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
                        logging.info("Block %i -- Calculated market cap history point for %s as %s (mID: %s)" % (t['block_index'], asset, market_cap_as, t['message_index']))

    #all done for this run...call again in a bit                            
    gevent.spawn_later(COMPILE_ASSET_MARKET_INFO_PERIOD, compile_asset_market_info)
    mongo_db.app_config.update({}, {'$set': {'last_block_assets_compiled': current_block_index}})
    