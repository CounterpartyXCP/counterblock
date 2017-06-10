"""
Implements counterwallet asset-related support as a counterblock plugin

DEPENDENCIES: This module requires the assets module to be loaded before it.

Python 2.x, as counterblock is still python 2.x
"""
import os
import sys
import time
import datetime
import logging
import decimal
import urllib.request
import urllib.parse
import urllib.error
import json
import operator
import base64
import configparser
import calendar

import pymongo
from bson.son import SON
import dateutil.parser

from counterblock.lib import config, util, blockfeed, blockchain
from counterblock.lib.modules import DEX_PRIORITY_PARSE_TRADEBOOK
from counterblock.lib.processor import MessageProcessor, MempoolMessageProcessor, BlockProcessor, StartUpProcessor, CaughtUpProcessor, RollbackProcessor, API, start_task
from . import assets_trading, dex

D = decimal.Decimal
EIGHT_PLACES = decimal.Decimal(10) ** -8

COMPILE_MARKET_PAIR_INFO_PERIOD = 10 * 60  # in seconds (this is every 10 minutes currently)
COMPILE_ASSET_MARKET_INFO_PERIOD = 30 * 60  # in seconds (this is every 30 minutes currently)

logger = logging.getLogger(__name__)


@API.add_method
def get_market_price_summary(asset1, asset2, with_last_trades=0):
    # DEPRECATED 1.5
    result = assets_trading.get_market_price_summary(asset1, asset2, with_last_trades)
    return result if result is not None else False
    #^ due to current bug in our jsonrpc stack, just return False if None is returned


@API.add_method
def get_market_cap_history(start_ts=None, end_ts=None):
    now_ts = calendar.timegm(time.gmtime())
    if not end_ts:  # default to current datetime
        end_ts = now_ts
    if not start_ts:  # default to 30 days before the end date
        start_ts = end_ts - (30 * 24 * 60 * 60)

    data = {}
    results = {}
    #^ format is result[market_cap_as][asset] = [[block_time, market_cap], [block_time2, market_cap2], ...]
    for market_cap_as in (config.XCP, config.BTC):
        caps = config.mongo_db.asset_marketcap_history.aggregate([
            {"$match": {
                "market_cap_as": market_cap_as,
                "block_time": {
                    "$gte": datetime.datetime.utcfromtimestamp(start_ts)
                } if end_ts == now_ts else {
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
                "market_cap": {"$avg": "$market_cap"},  # use the average marketcap during the interval
            }},
        ])
        data[market_cap_as] = {}
        for e in caps:
            interval_time = int(calendar.timegm(datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day'], e['_id']['hour']).timetuple()) * 1000)
            data[market_cap_as].setdefault(e['_id']['asset'], [])
            data[market_cap_as][e['_id']['asset']].append([interval_time, e['market_cap']])
        results[market_cap_as] = []
        for asset in data[market_cap_as]:
            #for z in data[market_cap_as][asset]: assert z[0] and z[0] > 0 and z[1] and z[1] >= 0
            results[market_cap_as].append(
                {'name': asset, 'data': sorted(data[market_cap_as][asset], key=operator.itemgetter(0))})
    return results


@API.add_method
def get_market_info(assets):
    assets_market_info = list(config.mongo_db.asset_market_info.find({'asset': {'$in': assets}}, {'_id': 0}))
    extended_asset_info = config.mongo_db.asset_extended_info.find({'asset': {'$in': assets}})
    extended_asset_info_dict = {}
    for e in extended_asset_info:
        if not e.get('disabled', False):  # skip assets marked disabled
            extended_asset_info_dict[e['asset']] = e
    for a in assets_market_info:
        if a['asset'] in extended_asset_info_dict and extended_asset_info_dict[a['asset']].get('processed', False):
            extended_info = extended_asset_info_dict[a['asset']]
            a['extended_image'] = bool(extended_info.get('image', ''))
            a['extended_description'] = extended_info.get('description', '')
            a['extended_website'] = extended_info.get('website', '')
            a['extended_pgpsig'] = extended_info.get('pgpsig', '')
        else:
            a['extended_image'] = a['extended_description'] = a['extended_website'] = a['extended_pgpsig'] = ''
    return assets_market_info


@API.add_method
def get_market_info_leaderboard(limit=100):
    """returns market leaderboard data for both the XCP and BTC markets"""
    # do two queries because we limit by our sorted results, and we might miss an asset with a high BTC trading value
    # but with little or no XCP trading activity, for instance if we just did one query
    assets_market_info_xcp = list(config.mongo_db.asset_market_info.find({}, {'_id': 0}).sort('market_cap_in_{}'.format(config.XCP.lower()), pymongo.DESCENDING).limit(limit))
    assets_market_info_btc = list(config.mongo_db.asset_market_info.find({}, {'_id': 0}).sort('market_cap_in_{}'.format(config.BTC.lower()), pymongo.DESCENDING).limit(limit))
    assets_market_info = {
        config.XCP.lower(): [a for a in assets_market_info_xcp if a['price_in_{}'.format(config.XCP.lower())]],
        config.BTC.lower(): [a for a in assets_market_info_btc if a['price_in_{}'.format(config.BTC.lower())]]
    }
    # throw on extended info, if it exists for a given asset
    assets = list(set([a['asset'] for a in assets_market_info[config.XCP.lower()]] + [a['asset'] for a in assets_market_info[config.BTC.lower()]]))
    extended_asset_info = config.mongo_db.asset_extended_info.find({'asset': {'$in': assets}})
    extended_asset_info_dict = {}
    for e in extended_asset_info:
        if not e.get('disabled', False):  # skip assets marked disabled
            extended_asset_info_dict[e['asset']] = e
    for r in (assets_market_info[config.XCP.lower()], assets_market_info[config.BTC.lower()]):
        for a in r:
            if a['asset'] in extended_asset_info_dict:
                extended_info = extended_asset_info_dict[a['asset']]
                if 'extended_image' not in a or 'extended_description' not in a or 'extended_website' not in a:
                    continue  # asset has been recognized as having a JSON file description, but has not been successfully processed yet
                a['extended_image'] = bool(extended_info.get('image', ''))
                a['extended_description'] = extended_info.get('description', '')
                a['extended_website'] = extended_info.get('website', '')
            else:
                a['extended_image'] = a['extended_description'] = a['extended_website'] = ''
    return assets_market_info


@API.add_method
def get_market_price_history(asset1, asset2, start_ts=None, end_ts=None, as_dict=False):
    """Return block-by-block aggregated market history data for the specified asset pair, within the specified date range.
    @returns List of lists (or list of dicts, if as_dict is specified).
        * If as_dict is False, each embedded list has 8 elements [block time (epoch in MS), open, high, low, close, volume, # trades in block, block index]
        * If as_dict is True, each dict in the list has the keys: block_time (epoch in MS), block_index, open, high, low, close, vol, count

    Aggregate on an an hourly basis 
    """
    now_ts = calendar.timegm(time.gmtime())
    if not end_ts:  # default to current datetime
        end_ts = now_ts
    if not start_ts:  # default to 180 days before the end date
        start_ts = end_ts - (180 * 24 * 60 * 60)
    base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)

    # get ticks -- open, high, low, close, volume
    result = config.mongo_db.trades.aggregate([
        {"$match": {
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "block_time": {
                "$gte": datetime.datetime.utcfromtimestamp(start_ts)
            } if end_ts == now_ts else {
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
            "base_quantity_normalized": 1  # to derive volume
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
    result = list(result)
    if not len(result):
        return False

    midline = [((r['high'] + r['low']) / 2.0) for r in result]
    if as_dict:
        for i in range(len(result)):
            result[i]['interval_time'] = int(calendar.timegm(datetime.datetime(
                result[i]['_id']['year'], result[i]['_id']['month'], result[i]['_id']['day'], result[i]['_id']['hour']).timetuple()) * 1000)
            result[i]['midline'] = midline[i]
            del result[i]['_id']
        return result
    else:
        list_result = []
        for i in range(len(result)):
            list_result.append([
                int(calendar.timegm(datetime.datetime(
                    result[i]['_id']['year'], result[i]['_id']['month'], result[i]['_id']['day'], result[i]['_id']['hour']).timetuple()) * 1000),
                result[i]['open'], result[i]['high'], result[i]['low'], result[i]['close'], result[i]['vol'],
                result[i]['count'], midline[i]
            ])
        return list_result


@API.add_method
def get_trade_history(asset1=None, asset2=None, start_ts=None, end_ts=None, limit=50):
    """
    Gets last N of trades within a specific date range (normally, for a specified asset pair, but this can
    be left blank to get any/all trades).
    """
    assert (asset1 and asset2) or (not asset1 and not asset2)  # cannot have one asset, but not the other

    if limit > 500:
        raise Exception("Requesting history of too many trades")

    now_ts = calendar.timegm(time.gmtime())
    if not end_ts:  # default to current datetime
        end_ts = now_ts
    if not start_ts:  # default to 30 days before the end date
        start_ts = end_ts - (30 * 24 * 60 * 60)

    filters = {
        "block_time": {
            "$gte": datetime.datetime.utcfromtimestamp(start_ts)
        } if end_ts == now_ts else {
            "$gte": datetime.datetime.utcfromtimestamp(start_ts),
            "$lte": datetime.datetime.utcfromtimestamp(end_ts)
        }
    }
    if asset1 and asset2:
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        filters["base_asset"] = base_asset
        filters["quote_asset"] = quote_asset

    last_trades = config.mongo_db.trades.find(filters, {'_id': 0}).sort("block_time", pymongo.DESCENDING).limit(limit)
    if not last_trades.count():
        return False  # no suitable trade data to form a market price
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
    base_asset_info = config.mongo_db.tracked_assets.find_one({'asset': base_asset})
    quote_asset_info = config.mongo_db.tracked_assets.find_one({'asset': quote_asset})

    if not base_asset_info or not quote_asset_info:
        raise Exception("Invalid asset(s)")

    # TODO: limit # results to 8 or so for each book (we have to sort as well to limit)
    base_bid_filters = [
        {"field": "get_asset", "op": "==", "value": base_asset},
        {"field": "give_asset", "op": "==", "value": quote_asset},
    ]
    base_ask_filters = [
        {"field": "get_asset", "op": "==", "value": quote_asset},
        {"field": "give_asset", "op": "==", "value": base_asset},
    ]
    if base_asset == config.BTC or quote_asset == config.BTC:
        extra_filters = [
            {'field': 'give_remaining', 'op': '>', 'value': 0},  # don't show empty BTC orders
            {'field': 'get_remaining', 'op': '>', 'value': 0},  # don't show empty BTC orders
            {'field': 'fee_required_remaining', 'op': '>=', 'value': 0},
            {'field': 'fee_provided_remaining', 'op': '>=', 'value': 0},
        ]
        base_bid_filters += extra_filters
        base_ask_filters += extra_filters

    base_bid_orders = util.call_jsonrpc_api(
        "get_orders", {
            'filters': base_bid_filters,
            'show_expired': False,
            'status': 'open',
            'order_by': 'block_index',
            'order_dir': 'asc',
        }, abort_on_error=True)['result']

    base_ask_orders = util.call_jsonrpc_api(
        "get_orders", {
            'filters': base_ask_filters,
            'show_expired': False,
            'status': 'open',
            'order_by': 'block_index',
            'order_dir': 'asc',
        }, abort_on_error=True)['result']

    def get_o_pct(o):
        if o['give_asset'] == config.BTC:  # NB: fee_provided could be zero here
            pct_fee_provided = float((D(o['fee_provided_remaining']) / D(o['give_quantity'])))
        else:
            pct_fee_provided = None
        if o['get_asset'] == config.BTC:  # NB: fee_required could be zero here
            pct_fee_required = float((D(o['fee_required_remaining']) / D(o['get_quantity'])))
        else:
            pct_fee_required = None
        return pct_fee_provided, pct_fee_required

    # filter results by pct_fee_provided and pct_fee_required for BTC pairs as appropriate
    filtered_base_bid_orders = []
    filtered_base_ask_orders = []
    if base_asset == config.BTC or quote_asset == config.BTC:
        for o in base_bid_orders:
            pct_fee_provided, pct_fee_required = get_o_pct(o)
            addToBook = True
            if bid_book_min_pct_fee_provided is not None and pct_fee_provided is not None and pct_fee_provided < bid_book_min_pct_fee_provided:
                addToBook = False
            if bid_book_min_pct_fee_required is not None and pct_fee_required is not None and pct_fee_required < bid_book_min_pct_fee_required:
                addToBook = False
            if bid_book_max_pct_fee_required is not None and pct_fee_required is not None and pct_fee_required > bid_book_max_pct_fee_required:
                addToBook = False
            if addToBook:
                filtered_base_bid_orders.append(o)
        for o in base_ask_orders:
            pct_fee_provided, pct_fee_required = get_o_pct(o)
            addToBook = True
            if ask_book_min_pct_fee_provided is not None and pct_fee_provided is not None and pct_fee_provided < ask_book_min_pct_fee_provided:
                addToBook = False
            if ask_book_min_pct_fee_required is not None and pct_fee_required is not None and pct_fee_required < ask_book_min_pct_fee_required:
                addToBook = False
            if ask_book_max_pct_fee_required is not None and pct_fee_required is not None and pct_fee_required > ask_book_max_pct_fee_required:
                addToBook = False
            if addToBook:
                filtered_base_ask_orders.append(o)
    else:
        filtered_base_bid_orders += base_bid_orders
        filtered_base_ask_orders += base_ask_orders

    def make_book(orders, isBidBook):
        book = {}
        for o in orders:
            if o['give_asset'] == base_asset:
                if base_asset == config.BTC and o['give_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF:
                    continue  # filter dust orders, if necessary

                give_quantity = blockchain.normalize_quantity(o['give_quantity'], base_asset_info['divisible'])
                get_quantity = blockchain.normalize_quantity(o['get_quantity'], quote_asset_info['divisible'])
                unit_price = float((D(get_quantity) / D(give_quantity)))
                remaining = blockchain.normalize_quantity(o['give_remaining'], base_asset_info['divisible'])
            else:
                if quote_asset == config.BTC and o['give_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF:
                    continue  # filter dust orders, if necessary

                give_quantity = blockchain.normalize_quantity(o['give_quantity'], quote_asset_info['divisible'])
                get_quantity = blockchain.normalize_quantity(o['get_quantity'], base_asset_info['divisible'])
                unit_price = float((D(give_quantity) / D(get_quantity)))
                remaining = blockchain.normalize_quantity(o['get_remaining'], base_asset_info['divisible'])
            id = "%s_%s_%s" % (base_asset, quote_asset, unit_price)
            #^ key = {base}_{bid}_{unit_price}, values ref entries in book
            book.setdefault(id, {'unit_price': unit_price, 'quantity': 0, 'count': 0})
            book[id]['quantity'] += remaining  # base quantity outstanding
            book[id]['count'] += 1  # num orders at this price level
        book = sorted(iter(book.values()), key=operator.itemgetter('unit_price'), reverse=isBidBook)
        #^ convert to list and sort -- bid book = descending, ask book = ascending
        return book

    # compile into a single book, at volume tiers
    base_bid_book = make_book(filtered_base_bid_orders, True)
    base_ask_book = make_book(filtered_base_ask_orders, False)

    # get stats like the spread and median
    if base_bid_book and base_ask_book:
        # don't do abs(), as this is "the amount by which the ask price exceeds the bid", so I guess it could be negative
        # if there is overlap in the book (right?)
        bid_ask_spread = float((D(base_ask_book[0]['unit_price']) - D(base_bid_book[0]['unit_price'])))
        bid_ask_median = float((D(max(base_ask_book[0]['unit_price'], base_bid_book[0]['unit_price'])) - (D(abs(bid_ask_spread)) / 2)))
    else:
        bid_ask_spread = 0
        bid_ask_median = 0

    # compose depth and round out quantities
    bid_depth = D(0)
    for o in base_bid_book:
        o['quantity'] = float(D(o['quantity']))
        bid_depth += D(o['quantity'])
        o['depth'] = float(D(bid_depth))
    bid_depth = float(D(bid_depth))
    ask_depth = D(0)
    for o in base_ask_book:
        o['quantity'] = float(D(o['quantity']))
        ask_depth += D(o['quantity'])
        o['depth'] = float(D(ask_depth))
    ask_depth = float(D(ask_depth))

    # compose raw orders
    orders = filtered_base_bid_orders + filtered_base_ask_orders
    for o in orders:
        # add in the blocktime to help makes interfaces more user-friendly (i.e. avoid displaying block
        # indexes and display datetimes instead)
        o['block_time'] = calendar.timegm(util.get_block_time(o['block_index']).timetuple()) * 1000

    result = {
        'base_bid_book': base_bid_book,
        'base_ask_book': base_ask_book,
        'bid_depth': bid_depth,
        'ask_depth': ask_depth,
        'bid_ask_spread': bid_ask_spread,
        'bid_ask_median': bid_ask_median,
        'raw_orders': orders,
        'base_asset': base_asset,
        'quote_asset': quote_asset
    }
    return result


@API.add_method
def get_order_book_simple(asset1, asset2, min_pct_fee_provided=None, max_pct_fee_required=None):
    # DEPRECATED 1.5
    base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
    result = _get_order_book(
        base_asset, quote_asset,
        bid_book_min_pct_fee_provided=min_pct_fee_provided,
        bid_book_max_pct_fee_required=max_pct_fee_required,
        ask_book_min_pct_fee_provided=min_pct_fee_provided,
        ask_book_max_pct_fee_required=max_pct_fee_required)
    return result


@API.add_method
def get_order_book_buysell(buy_asset, sell_asset, pct_fee_provided=None, pct_fee_required=None):
    # DEPRECATED 1.5
    base_asset, quote_asset = util.assets_to_asset_pair(buy_asset, sell_asset)
    bid_book_min_pct_fee_provided = None
    bid_book_min_pct_fee_required = None
    bid_book_max_pct_fee_required = None
    ask_book_min_pct_fee_provided = None
    ask_book_min_pct_fee_required = None
    ask_book_max_pct_fee_required = None
    if base_asset == config.BTC:
        if buy_asset == config.BTC:
            # if BTC is base asset and we're buying it, we're buying the BASE. we require a BTC fee (we're on the bid (bottom) book and we want a lower price)
            # - show BASE buyers (bid book) that require a BTC fee >= what we require (our side of the book)
            # - show BASE sellers (ask book) that provide a BTC fee >= what we require
            bid_book_min_pct_fee_required = pct_fee_required  # my competition at the given fee required
            ask_book_min_pct_fee_provided = pct_fee_required
        elif sell_asset == config.BTC:
            # if BTC is base asset and we're selling it, we're selling the BASE. we provide a BTC fee (we're on the ask (top) book and we want a higher price)
            # - show BASE buyers (bid book) that provide a BTC fee >= what we provide
            # - show BASE sellers (ask book) that require a BTC fee <= what we provide (our side of the book)
            bid_book_max_pct_fee_required = pct_fee_provided
            ask_book_min_pct_fee_provided = pct_fee_provided  # my competition at the given fee provided
    elif quote_asset == config.BTC:
        assert base_asset == config.XCP  # only time when this is the case
        if buy_asset == config.BTC:
            # if BTC is quote asset and we're buying it, we're selling the BASE. we require a BTC fee (we're on the ask (top) book and we want a higher price)
            # - show BASE buyers (bid book) that provide a BTC fee >= what we require
            # - show BASE sellers (ask book) that require a BTC fee >= what we require (our side of the book)
            bid_book_min_pct_fee_provided = pct_fee_required
            ask_book_min_pct_fee_required = pct_fee_required  # my competition at the given fee required
        elif sell_asset == config.BTC:
            # if BTC is quote asset and we're selling it, we're buying the BASE. we provide a BTC fee (we're on the bid (bottom) book and we want a lower price)
            # - show BASE buyers (bid book) that provide a BTC fee >= what we provide (our side of the book)
            # - show BASE sellers (ask book) that require a BTC fee <= what we provide
            bid_book_min_pct_fee_provided = pct_fee_provided  # my compeitition at the given fee provided
            ask_book_max_pct_fee_required = pct_fee_provided

    result = _get_order_book(
        base_asset, quote_asset,
        bid_book_min_pct_fee_provided=bid_book_min_pct_fee_provided,
        bid_book_min_pct_fee_required=bid_book_min_pct_fee_required,
        bid_book_max_pct_fee_required=bid_book_max_pct_fee_required,
        ask_book_min_pct_fee_provided=ask_book_min_pct_fee_provided,
        ask_book_min_pct_fee_required=ask_book_min_pct_fee_required,
        ask_book_max_pct_fee_required=ask_book_max_pct_fee_required)

    # filter down raw_orders to be only open sell orders for what the caller is buying
    open_sell_orders = []
    for o in result['raw_orders']:
        if o['give_asset'] == buy_asset:
            open_sell_orders.append(o)
    result['raw_orders'] = open_sell_orders
    return result


@API.add_method
def get_users_pairs(addresses=[], max_pairs=12):
    return dex.get_users_pairs(addresses, max_pairs, quote_assets=[config.XCP, config.XBTC])


@API.add_method
def get_market_orders(asset1, asset2, addresses=[], min_fee_provided=0.95, max_fee_required=0.95):
    return dex.get_market_orders(asset1, asset2, addresses, None, min_fee_provided, max_fee_required)


@API.add_method
def get_market_trades(asset1, asset2, addresses=[], limit=50):
    return dex.get_market_trades(asset1, asset2, addresses, limit)


@API.add_method
def get_markets_list(quote_asset=None, order_by=None):
    return dex.get_markets_list(quote_asset=quote_asset, order_by=order_by)


@API.add_method
def get_market_details(asset1, asset2, min_fee_provided=0.95, max_fee_required=0.95):
    return dex.get_market_details(asset1, asset2, min_fee_provided, max_fee_required)


def task_compile_asset_pair_market_info():
    assets_trading.compile_asset_pair_market_info()
    # all done for this run...call again in a bit
    start_task(task_compile_asset_pair_market_info, delay=COMPILE_MARKET_PAIR_INFO_PERIOD)


def task_compile_asset_market_info():
    assets_trading.compile_asset_market_info()
    # all done for this run...call again in a bit
    start_task(task_compile_asset_market_info, delay=COMPILE_ASSET_MARKET_INFO_PERIOD)


@MessageProcessor.subscribe(priority=DEX_PRIORITY_PARSE_TRADEBOOK)
def parse_trade_book(msg, msg_data):
    # book trades
    if(msg['category'] == 'order_matches' and
       ((msg['command'] == 'update' and msg_data['status'] == 'completed') or  # for a trade with BTC involved, but that is settled (completed)
        ('forward_asset' in msg_data and msg_data['forward_asset'] != config.BTC and msg_data['backward_asset'] != config.BTC)
        )
       ):  # or for a trade without BTC on either end

        if msg['command'] == 'update' and msg_data['status'] == 'completed':
            # an order is being updated to a completed status (i.e. a BTCpay has completed)
            tx0_hash, tx1_hash = msg_data['order_match_id'][:64], msg_data['order_match_id'][65:]
            # get the order_match this btcpay settles
            order_match = util.jsonrpc_api(
                "get_order_matches",
                {'filters': [
                 {'field': 'tx0_hash', 'op': '==', 'value': tx0_hash},
                 {'field': 'tx1_hash', 'op': '==', 'value': tx1_hash}]
                 }, abort_on_error=False)['result'][0]
        else:
            assert msg_data['status'] == 'completed'  # should not enter a pending state for non BTC matches
            order_match = msg_data

        forward_asset_info = config.mongo_db.tracked_assets.find_one({'asset': order_match['forward_asset']})
        backward_asset_info = config.mongo_db.tracked_assets.find_one({'asset': order_match['backward_asset']})
        assert forward_asset_info and backward_asset_info
        base_asset, quote_asset = util.assets_to_asset_pair(order_match['forward_asset'], order_match['backward_asset'])

        # don't create trade records from order matches with BTC that are under the dust limit
        if((order_match['forward_asset'] == config.BTC and
            order_match['forward_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF)
           or (order_match['backward_asset'] == config.BTC and
               order_match['backward_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF)):
            logger.debug("Order match %s ignored due to %s under dust limit." % (order_match['tx0_hash'] + order_match['tx1_hash'], config.BTC))
            return 'ABORT_THIS_MESSAGE_PROCESSING'

        # take divisible trade quantities to floating point
        forward_quantity = blockchain.normalize_quantity(order_match['forward_quantity'], forward_asset_info['divisible'])
        backward_quantity = blockchain.normalize_quantity(order_match['backward_quantity'], backward_asset_info['divisible'])

        # compose trade
        trade = {
            'block_index': config.state['cur_block']['block_index'],
            'block_time': config.state['cur_block']['block_time_obj'],
            'message_index': msg['message_index'],  # secondary temporaral ordering off of when
            'order_match_id': order_match['tx0_hash'] + '_' + order_match['tx1_hash'],
            'order_match_tx0_index': order_match['tx0_index'],
            'order_match_tx1_index': order_match['tx1_index'],
            'order_match_tx0_address': order_match['tx0_address'],
            'order_match_tx1_address': order_match['tx1_address'],
            'base_asset': base_asset,
            'quote_asset': quote_asset,
            'base_quantity': order_match['forward_quantity'] if order_match['forward_asset'] == base_asset else order_match['backward_quantity'],
            'quote_quantity': order_match['backward_quantity'] if order_match['forward_asset'] == base_asset else order_match['forward_quantity'],
            'base_quantity_normalized': forward_quantity if order_match['forward_asset'] == base_asset else backward_quantity,
            'quote_quantity_normalized': backward_quantity if order_match['forward_asset'] == base_asset else forward_quantity,
        }
        d = D(trade['quote_quantity_normalized']) / D(trade['base_quantity_normalized'])
        d = d.quantize(EIGHT_PLACES, rounding=decimal.ROUND_HALF_EVEN, context=decimal.Context(prec=30))
        trade['unit_price'] = float(d)

        d = D(trade['base_quantity_normalized']) / D(trade['quote_quantity_normalized'])
        d = d.quantize(EIGHT_PLACES, rounding=decimal.ROUND_HALF_EVEN, context=decimal.Context(prec=30))
        trade['unit_price_inverse'] = float(d)

        config.mongo_db.trades.insert(trade)
        logger.info("Procesed Trade from tx %s :: %s" % (msg['message_index'], trade))


@StartUpProcessor.subscribe()
def init():
    # init db and indexes
    # trades
    config.mongo_db.trades.ensure_index(
        [("base_asset", pymongo.ASCENDING),
         ("quote_asset", pymongo.ASCENDING),
         ("block_time", pymongo.DESCENDING)
         ])
    config.mongo_db.trades.ensure_index(  # tasks.py and elsewhere (for singlular block_index index access)
        [("block_index", pymongo.ASCENDING),
         ("base_asset", pymongo.ASCENDING),
         ("quote_asset", pymongo.ASCENDING)
         ])
    # asset_market_info
    config.mongo_db.asset_market_info.ensure_index('asset', unique=True)
    # asset_marketcap_history
    config.mongo_db.asset_marketcap_history.ensure_index('block_index')
    config.mongo_db.asset_marketcap_history.ensure_index(  # tasks.py
        [
            ("market_cap_as", pymongo.ASCENDING),
            ("asset", pymongo.ASCENDING),
            ("block_index", pymongo.DESCENDING)
        ])
    config.mongo_db.asset_marketcap_history.ensure_index(  # api.py
        [
            ("market_cap_as", pymongo.ASCENDING),
            ("block_time", pymongo.DESCENDING)
        ])
    # asset_pair_market_info
    config.mongo_db.asset_pair_market_info.ensure_index(  # event.py, api.py
        [("base_asset", pymongo.ASCENDING),
         ("quote_asset", pymongo.ASCENDING)
         ], unique=True)
    config.mongo_db.asset_pair_market_info.ensure_index('last_updated')


@CaughtUpProcessor.subscribe()
def start_tasks():
    start_task(task_compile_asset_pair_market_info)
    start_task(task_compile_asset_market_info)


@RollbackProcessor.subscribe()
def process_rollback(max_block_index):
    if not max_block_index:  # full reparse
        config.mongo_db.trades.drop()
        config.mongo_db.asset_market_info.drop()
        config.mongo_db.asset_marketcap_history.drop()
        config.mongo_db.pair_market_info.drop()
    else:  # rollback
        config.mongo_db.trades.remove({"block_index": {"$gt": max_block_index}})
        config.mongo_db.asset_marketcap_history.remove({"block_index": {"$gt": max_block_index}})
