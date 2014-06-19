import os
import re
import json
import base64
import logging
import datetime
import time
import copy
import decimal
import cgi

import numpy
import pymongo

from . import (config, util)

D = decimal.Decimal

def get_market_price(price_data, vol_data):
    assert len(price_data) == len(vol_data)
    assert len(price_data) <= config.MARKET_PRICE_DERIVE_NUM_POINTS
    market_price = numpy.average(price_data, weights=vol_data)
    return market_price

def get_market_price_summary(asset1, asset2, with_last_trades=0, start_dt=None, end_dt=None):
    """Gets a synthesized trading "market price" for a specified asset pair (if available), as well as additional info.
    If no price is available, False is returned.
    """
    mongo_db = config.mongo_db
    if not end_dt:
        end_dt = datetime.datetime.utcnow()
    if not start_dt:
        start_dt = end_dt - datetime.timedelta(days=10) #default to 10 days in the past
    
    #look for the last max 6 trades within the past 10 day window
    base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
    base_asset_info = mongo_db.tracked_assets.find_one({'asset': base_asset})
    quote_asset_info = mongo_db.tracked_assets.find_one({'asset': quote_asset})
    
    if not isinstance(with_last_trades, int) or with_last_trades < 0 or with_last_trades > 30:
        raise Exception("Invalid with_last_trades")
    
    if not base_asset_info or not quote_asset_info:
        raise Exception("Invalid asset(s)")
    
    last_trades = mongo_db.trades.find({
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            'block_time': { "$gte": start_dt, "$lte": end_dt }
        },
        {'_id': 0, 'block_index': 1, 'block_time': 1, 'unit_price': 1, 'base_quantity_normalized': 1, 'quote_quantity_normalized': 1}
    ).sort("block_time", pymongo.DESCENDING).limit(max(config.MARKET_PRICE_DERIVE_NUM_POINTS, with_last_trades))
    if not last_trades.count():
        return None #no suitable trade data to form a market price (return None, NOT False here)
    last_trades = list(last_trades)
    last_trades.reverse() #from newest to oldest
    
    market_price = get_market_price(
        [last_trades[i]['unit_price'] for i in xrange(min(len(last_trades), config.MARKET_PRICE_DERIVE_NUM_POINTS))],
        [(last_trades[i]['base_quantity_normalized'] + last_trades[i]['quote_quantity_normalized']) for i in xrange(min(len(last_trades), config.MARKET_PRICE_DERIVE_NUM_POINTS))])
    result = {
        'market_price': float(D(market_price).quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN)),
        'base_asset': base_asset,
        'quote_asset': quote_asset,
    }
    if with_last_trades:
        #[0]=block_time, [1]=unit_price, [2]=base_quantity_normalized, [3]=quote_quantity_normalized, [4]=block_index
        result['last_trades'] = [[
            t['block_time'],
            t['unit_price'],
            t['base_quantity_normalized'],
            t['quote_quantity_normalized'],
            t['block_index']
        ] for t in last_trades]
    else:
        result['last_trades'] = []
    return result


def calc_inverse(quantity):
    return float( (D(1) / D(quantity) ).quantize(
        D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))            

def calc_price_change(open, close):
    return float((D(100) * (D(close) - D(open)) / D(open)).quantize(
            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))            

def get_price_primatives(start_dt=None, end_dt=None):
    mps_xcp_btc = get_market_price_summary(config.XCP, config.BTC, start_dt=start_dt, end_dt=end_dt)
    xcp_btc_price = mps_xcp_btc['market_price'] if mps_xcp_btc else None # == XCP/BTC
    btc_xcp_price = calc_inverse(mps_xcp_btc['market_price']) if mps_xcp_btc else None #BTC/XCP
    return mps_xcp_btc, xcp_btc_price, btc_xcp_price

def get_asset_info(asset, at_dt=None):
    mongo_db = config.mongo_db
    asset_info = mongo_db.tracked_assets.find_one({'asset': asset})
    
    if asset not in (config.XCP, config.BTC) and at_dt and asset_info['_at_block_time'] > at_dt:
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
    if asset == config.BTC:
        if at_dt:
            start_block_index, end_block_index = util.get_block_indexes_for_dates(end_dt=at_dt)
            asset_info['total_issued'] = util.get_btc_supply(normalize=False, at_block_index=end_block_index)
            asset_info['total_issued_normalized'] = util.normalize_quantity(asset_info['total_issued'])
        else:
            asset_info['total_issued'] = util.get_btc_supply(normalize=False)
            asset_info['total_issued_normalized'] = util.normalize_quantity(asset_info['total_issued'])
    elif asset == config.XCP:
        #BUG: this does not take end_dt (if specified) into account. however, the deviation won't be too big
        # as XCP doesn't deflate quickly at all, and shouldn't matter that much since there weren't any/much trades
        # before the end of the burn period (which is what is involved with how we use at_dt with currently)
        asset_info['total_issued'] = util.call_jsonrpc_api("get_xcp_supply", [], abort_on_error=True)['result']
        asset_info['total_issued_normalized'] = util.normalize_quantity(asset_info['total_issued'])
    if not asset_info:
        raise Exception("Invalid asset: %s" % asset)
    return asset_info

def get_xcp_btc_price_info(asset, mps_xcp_btc, xcp_btc_price, btc_xcp_price, with_last_trades=0, start_dt=None, end_dt=None):
    if asset not in [config.BTC, config.XCP]:
        #get price data for both the asset with XCP, as well as BTC
        price_summary_in_xcp = get_market_price_summary(asset, config.XCP,
            with_last_trades=with_last_trades, start_dt=start_dt, end_dt=end_dt)
        price_summary_in_btc = get_market_price_summary(asset, config.BTC,
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
            price_summary_in_btc['base_asset'] = config.BTC
            price_summary_in_btc['quote_asset'] = config.XCP
            for i in xrange(len(price_summary_in_btc['last_trades'])):
                #[0]=block_time, [1]=unit_price, [2]=base_quantity_normalized, [3]=quote_quantity_normalized, [4]=block_index
                price_summary_in_btc['last_trades'][i][1] = calc_inverse(price_summary_in_btc['last_trades'][i][1])
                price_summary_in_btc['last_trades'][i][2], price_summary_in_btc['last_trades'][i][3] = \
                    price_summary_in_btc['last_trades'][i][3], price_summary_in_btc['last_trades'][i][2] #swap
        if asset == config.XCP:
            price_in_xcp = 1.0
            price_in_btc = price_summary_in_btc['market_price'] if price_summary_in_btc else None
            aggregated_price_in_xcp = 1.0
            aggregated_price_in_btc = btc_xcp_price #might be None
        else:
            assert asset == config.BTC
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

def compile_summary_market_info(asset, mps_xcp_btc, xcp_btc_price, btc_xcp_price):        
    """Returns information related to capitalization, volume, etc for the supplied asset(s)
    NOTE: in_btc == base asset is BTC, in_xcp == base asset is XCP
    @param assets: A list of one or more assets
    """
    asset_info = get_asset_info(asset)
    (price_summary_in_xcp, price_summary_in_btc, price_in_xcp, price_in_btc, aggregated_price_in_xcp, aggregated_price_in_btc
    ) = get_xcp_btc_price_info(asset, mps_xcp_btc, xcp_btc_price, btc_xcp_price, with_last_trades=30)
    market_cap_in_xcp, market_cap_in_btc = calc_market_cap(asset_info, price_in_xcp, price_in_btc)
    return {
        'price_in_{}'.format(config.XCP.lower()): price_in_xcp, #current price of asset vs XCP (e.g. how many units of asset for 1 unit XCP)
        'price_in_{}'.format(config.BTC.lower()): price_in_btc, #current price of asset vs BTC (e.g. how many units of asset for 1 unit BTC)
        'price_as_{}'.format(config.XCP.lower()): calc_inverse(price_in_xcp) if price_in_xcp else None, #current price of asset AS XCP
        'price_as_{}'.format(config.BTC.lower()): calc_inverse(price_in_btc) if price_in_btc else None, #current price of asset AS BTC
        'aggregated_price_in_{}'.format(config.XCP.lower()): aggregated_price_in_xcp, 
        'aggregated_price_in_{}'.format(config.BTC.lower()): aggregated_price_in_btc,
        'aggregated_price_as_{}'.format(config.XCP.lower()): calc_inverse(aggregated_price_in_xcp) if aggregated_price_in_xcp else None, 
        'aggregated_price_as_{}'.format(config.BTC.lower()): calc_inverse(aggregated_price_in_btc) if aggregated_price_in_btc else None,
        'total_supply': asset_info['total_issued_normalized'], 
        'market_cap_in_{}'.format(config.XCP.lower()): market_cap_in_xcp,
        'market_cap_in_{}'.format(config.BTC.lower()): market_cap_in_btc,
    }

def compile_24h_market_info(asset):        
    asset_data = {}
    start_dt_1d = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    mongo_db = config.mongo_db

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
    if asset != config.XCP:
        _24h_ohlc_in_xcp = mongo_db.trades.aggregate([
            {"$match": {
                "base_asset": config.XCP,
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
    if asset != config.BTC:
        _24h_ohlc_in_btc = mongo_db.trades.aggregate([
            {"$match": {
                "base_asset": config.BTC,
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
        
    return {
        '24h_summary': _24h_vols,
        #^ total quantity traded of that asset in all markets in last 24h
        '24h_ohlc_in_{}'.format(config.XCP.lower()): _24h_ohlc_in_xcp,
        #^ quantity of asset traded with BTC in last 24h
        '24h_ohlc_in_{}'.format(config.BTC.lower()): _24h_ohlc_in_btc,
        #^ quantity of asset traded with XCP in last 24h
        '24h_vol_price_change_in_{}'.format(config.XCP.lower()): calc_price_change(_24h_ohlc_in_xcp['open'], _24h_ohlc_in_xcp['close'])
            if _24h_ohlc_in_xcp else None,
        #^ aggregated price change from 24h ago to now, expressed as a signed float (e.g. .54 is +54%, -1.12 is -112%)
        '24h_vol_price_change_in_{}'.format(config.BTC.lower()): calc_price_change(_24h_ohlc_in_btc['open'], _24h_ohlc_in_btc['close'])
            if _24h_ohlc_in_btc else None,
    }

def compile_7d_market_info(asset): 
    mongo_db = config.mongo_db       
    start_dt_7d = datetime.datetime.utcnow() - datetime.timedelta(days=7)

    #get XCP and BTC market summarized trades over a 7d period (quantize to hour long slots)
    _7d_history_in_xcp = None # xcp/asset market (or xcp/btc for xcp or btc)
    _7d_history_in_btc = None # btc/asset market (or btc/xcp for xcp or btc)
    if asset not in [config.BTC, config.XCP]:
        for a in [config.XCP, config.BTC]:
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
            if a == config.XCP: _7d_history_in_xcp = _7d_history
            else: _7d_history_in_btc = _7d_history
    else: #get the XCP/BTC market and invert for BTC/XCP (_7d_history_in_btc)
        _7d_history = mongo_db.trades.aggregate([
            {"$match": {
                "base_asset": config.XCP,
                "quote_asset": config.BTC,
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

    return {
        '7d_history_in_{}'.format(config.XCP.lower()): [[e['when'], e['price']] for e in _7d_history_in_xcp],
        '7d_history_in_{}'.format(config.BTC.lower()): [[e['when'], e['price']] for e in _7d_history_in_btc],
    }
