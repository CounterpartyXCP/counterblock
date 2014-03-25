import os
import json
import base64
import logging
import datetime
import time
import copy
import decimal

import pymongo
import grequests

from . import (config,)

D = decimal.Decimal

def assets_to_asset_pair(asset1, asset2):
    """Pair labeling rules are:
    If XCP is either asset, it takes presidence as the base asset.
    If XCP is not either asset, but BTC is, BTC will take presidence as the base asset.
    If neither XCP nor BTC are either asset, the first asset (alphabetically) will take presidence as the base asset
    """
    base = None
    quote = None
    if asset1 == 'XCP' or asset2 == 'XCP':
        base = asset1 if asset1 == 'XCP' else asset2
        quote = asset2 if asset1 == 'XCP' else asset1
    elif asset1 == 'BTC' or asset2 == 'BTC':
        base = asset1 if asset1 == 'BTC' else asset2
        quote = asset2 if asset1 == 'BTC' else asset1
    else:
        base = asset1 if asset1 < asset2 else asset2
        quote = asset2 if asset1 < asset2 else asset1
    return (base, quote)

def call_jsonrpc_api(method, params=None, endpoint=None, auth=None, abort_on_error=False):
    if not endpoint: endpoint = config.COUNTERPARTYD_RPC
    if not auth: auth = config.COUNTERPARTYD_AUTH
    
    payload = {
      "id": 0,
      "jsonrpc": "2.0",
      "method": method,
      "params": params or [],
    }
    r = grequests.map(
        (grequests.post(endpoint,
            data=json.dumps(payload),
            headers={'content-type': 'application/json'},
            auth=auth),)
    )[0]
    #^ use requests.Session to utilize connectionpool and keepalive (avoid connection setup/teardown overhead)
    if not r:
        raise Exception("Could not contact counterpartyd!")
    elif r.status_code != 200:
        raise Exception("Bad status code returned from counterpartyd: '%s'. result body: '%s'." % (r.status_code, r.text))
    else:
        result = r.json()
    if abort_on_error and 'error' in result:
        raise Exception("Got back error from server: %s" % result['error'])
    return result

def call_insight_api(request_string, abort_on_error=False):
    r = grequests.map((grequests.get(config.INSIGHT + request_string),) )[0]
    #^ use requests.Session to utilize connectionpool and keepalive (avoid connection setup/teardown overhead)
    if not r and abort_on_error:
        raise Exception("Could not contact insight!")
    elif r.status_code != 200 and abort_on_error:
        raise Exception("Bad status code returned from insight: '%s'. result body: '%s'." % (r.status_code, r.text))
    else:
        try:
            result = r.json()
        except:
            if abort_on_error: raise 
            result = None
    return result

def get_address_cols_for_entity(entity):
    if entity in ['debits', 'credits']:
        return ['address',]
    elif entity in ['issuances',]:
        return ['issuer',]
    elif entity in ['sends', 'dividends', 'bets', 'cancels', 'callbacks', 'orders', 'burns', 'broadcasts', 'btcpays']:
        return ['source',]
    #elif entity in ['order_matches', 'bet_matches']:
    elif entity in ['order_matches', 'order_expirations', 'order_match_expirations',
                    'bet_matches', 'bet_expirations', 'bet_match_expirations']:
        return ['tx0_address', 'tx1_address']
    else:
        raise Exception("Unknown entity type: %s" % entity)


def multikeysort(items, columns):
    """http://stackoverflow.com/a/1144405"""
    from operator import itemgetter
    comparers = [ ((itemgetter(col[1:].strip()), -1) if col.startswith('-') else (itemgetter(col.strip()), 1)) for col in columns]  
    def comparer(left, right):
        for fn, mult in comparers:
            result = cmp(fn(left), fn(right))
            if result:
                return mult * result
        else:
            return 0
    return sorted(items, cmp=comparer)

def weighted_average(value_weight_list):
    """Takes a list of tuples (value, weight) and returns weighted average as
    calculated by Sum of all values * weights / Sum of all weights
    http://bcdcspatial.blogspot.com/2010/08/simple-weighted-average-with-python.html
    """    
    numerator = sum([v * w for v,w in value_weight_list])
    denominator = sum([w for v,w in value_weight_list])
    if(denominator != 0):
        return(float(numerator) / float(denominator))
    else:
        return None

def json_dthandler(obj):
    if hasattr(obj, 'timetuple'): #datetime object
        #give datetime objects to javascript as epoch ts in ms (i.e. * 1000)
        return int(time.mktime(obj.timetuple())) * 1000
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))

def get_block_indexes_for_dates(mongo_db, start_dt=None, end_dt=None):
    """Returns a 2 tuple (start_block, end_block) result for the block range that encompasses the given start_date
    and end_date unix timestamps"""
    if start_dt is None:
        start_block_index = config.BLOCK_FIRST
    else:
        start_block = mongo_db.processed_blocks.find_one({"block_time": {"$lte": start_dt} }, sort=[("block_time", pymongo.DESCENDING)])
        start_block_index = config.BLOCK_FIRST if not start_block else start_block['block_index']
    if end_dt is None:
        end_block_index = config.CURRENT_BLOCK_INDEX
    else:
        end_block = mongo_db.processed_blocks.find_one({"block_time": {"$gte": end_dt} }, sort=[("block_time", pymongo.ASCENDING)])
        if not end_block:
            end_block_index = mongo_db.processed_blocks.find_one(sort=[("block_index", pymongo.DESCENDING)])['block_index']
        else:
            end_block_index = end_block['block_index']
    return (start_block_index, end_block_index)

def get_block_time(mongo_db, block_index):
    """TODO: implement result caching to avoid having to go out to the database"""
    block = mongo_db.processed_blocks.find_one({"block_index": block_index })
    if not block: return None
    return block['block_time']

def get_market_price_summary(mongo_db, asset1, asset2, with_last_trades=0, start_dt=None, end_dt=None):
    """Gets a synthesized trading "market price" for a specified asset pair (if available), as well as additional info.
    If no price is available, False is returned.
    """
    MARKET_PRICE_DERIVE_NUMLAST = 6 #number of last trades over which to derive the market price
    MARKET_PRICE_DERIVE_WEIGHTS = [1, .9, .72, .6, .4, .3] #good first guess...maybe
    assert(len(MARKET_PRICE_DERIVE_WEIGHTS) == MARKET_PRICE_DERIVE_NUMLAST) #sanity check
    
    if not end_dt:
        end_dt = datetime.datetime.utcnow()
    if not start_dt:
        start_dt = end_dt - datetime.timedelta(days=10) #default to 10 days in the past
    
    #look for the last max 6 trades within the past 10 day window
    base_asset, quote_asset = assets_to_asset_pair(asset1, asset2)
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
    ).sort("block_time", pymongo.DESCENDING).limit(max(MARKET_PRICE_DERIVE_NUMLAST, with_last_trades))
    if not last_trades.count():
        return None #no suitable trade data to form a market price (return None, NOT False here)
    last_trades = list(last_trades)
    last_trades.reverse() #from newest to oldest
    weighted_inputs = []
    for i in xrange(min(len(last_trades), MARKET_PRICE_DERIVE_NUMLAST)):
        weighted_inputs.append([last_trades[i]['unit_price'], MARKET_PRICE_DERIVE_WEIGHTS[i]])
    market_price = weighted_average(weighted_inputs)
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

def create_message_feed_obj_from_cpd_message(mongo_db, msg, msg_data=None):
    """This function takes a message from counterpartyd's message feed and mutates it a bit to be suitable to be
    sent through the counterwalletd message feed to an end-client"""
    if not msg_data:
        msg_data = json.loads(msg['bindings'])
    
    event = copy.deepcopy(msg_data)
    event['_message_index'] = msg['message_index']
    event['_command'] = msg['command']
    event['_block_index'] = msg['block_index']
    event['_category'] = msg['category']
    event['_status'] = msg_data.get('status', 'valid')

    #insert custom fields in certain events...
    #even invalid actions need these extra fields for proper reporting to the client (as the reporting message
    # is produced via PendingActionViewModel.calcText) -- however make it able to deal with the queried data not existing in this case
    if(event['_category'] in ['credits', 'debits']):
        #find the last balance change on record
        bal_change = mongo_db.balance_changes.find_one({ 'address': event['address'], 'asset': event['asset'] },
            sort=[("block_time", pymongo.DESCENDING)])
        event['_quantity_normalized'] = abs(bal_change['quantity_normalized']) if bal_change else None
        event['_balance'] = bal_change['new_balance'] if bal_change else None
        event['_balance_normalized'] = bal_change['new_balance_normalized'] if bal_change else None
    elif(event['_category'] in ['orders',] and event['_command'] == 'insert'):
        get_asset_info = mongo_db.tracked_assets.find_one({'asset': event['get_asset']})
        give_asset_info = mongo_db.tracked_assets.find_one({'asset': event['give_asset']})
        event['_get_asset_divisible'] = get_asset_info['divisible'] if get_asset_info else None
        event['_give_asset_divisible'] = give_asset_info['divisible'] if give_asset_info else None
    elif(event['_category'] in ['order_matches',] and event['_command'] == 'insert'):
        forward_asset_info = mongo_db.tracked_assets.find_one({'asset': event['forward_asset']})
        backward_asset_info = mongo_db.tracked_assets.find_one({'asset': event['backward_asset']})
        event['_forward_asset_divisible'] = forward_asset_info['divisible'] if forward_asset_info else None
        event['_backward_asset_divisible'] = backward_asset_info['divisible'] if backward_asset_info else None
    elif(event['_category'] in ['dividends', 'sends',]):
        asset_info = mongo_db.tracked_assets.find_one({'asset': event['asset']})
        event['_divisible'] = asset_info['divisible'] if asset_info else None
    elif(event['_category'] in ['issuances',]):
        event['_quantity_normalized'] = normalize_quantity(msg_data['quantity'], msg_data['divisible'])
    return event


#############
# Bitcoin-related

def normalize_quantity(quantity, divisible=True):
    if divisible:
        return float((D(quantity) / D(config.UNIT)).quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN)) 
    else: return quantity

def denormalize_quantity(quantity, divisible=True):
    if divisible:
        return int(quantity * config.UNIT)
    else: return quantity

def get_btc_supply(normalize=False, at_block_index=None):
    """returns the total supply of BTC (based on what bitcoind says the current block height is)"""
    block_count = config.CURRENT_BLOCK_INDEX if at_block_index is None else at_block_index
    blocks_remaining = block_count
    total_supply = 0 
    reward = 50.0
    while blocks_remaining > 0:
        if blocks_remaining >= 210000:
            blocks_remaining -= 210000
            total_supply += 210000 * reward
            reward /= 2
        else:
            total_supply += (blocks_remaining * reward)
            blocks_remaining = 0
            
    return total_supply if normalize else int(total_supply * config.UNIT)
