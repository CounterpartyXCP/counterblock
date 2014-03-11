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
from requests import Session

from . import (config,)

D = decimal.Decimal
API_SESSION = Session()
API_INSIGHT_SESSION = Session()

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
            auth=auth,
            session=API_SESSION),)
    )[0]
    #^ use requests.Session to utilize connectionpool and keepalive (avoid connection setup/teardown overhead)
    if r.status_code != 200:
        raise Exception("Bad status code returned from counterwalletd: '%s'. result body: '%s'." % (r.status_code, r.text))
        result = None
    else:
        result = r.json()
    if abort_on_error and 'error' in result:
        raise Exception("Got back error from server: %s" % result['error'])
    return result

def call_insight_api(request_string, abort_on_error=False):
    r = grequests.map((grequests.get(config.INSIGHT + request_string, session=API_INSIGHT_SESSION),) )[0]
    #^ use requests.Session to utilize connectionpool and keepalive (avoid connection setup/teardown overhead)
    if r.status_code != 200:
        raise Exception("Bad status code returned from insight: '%s'. result body: '%s'." % (r.status_code, r.text))
        result = None
    else:
        result = r.json()
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
    #event['_block_time'] = msg['block_time']
    
    #insert custom fields in certain events...
    if(event['_category'] in ['credits', 'debits']):
        #find the last balance change on record
        bal_change = mongo_db.balance_changes.find_one({ 'address': event['address'], 'asset': event['asset'] },
            sort=[("block_time", pymongo.DESCENDING)])
        assert bal_change
        event['_amount_normalized'] = bal_change['amount_normalized']
        event['_balance'] = bal_change['new_balance']
        event['_balance_normalized'] = bal_change['new_balance_normalized']
    elif(event['_category'] in ['orders',] and event['_command'] == 'insert'):
        get_asset_info = mongo_db.tracked_assets.find_one({'asset': event['get_asset']})
        give_asset_info = mongo_db.tracked_assets.find_one({'asset': event['give_asset']})
        assert get_asset_info and give_asset_info
        event['_get_asset_divisible'] = get_asset_info['divisible']
        event['_give_asset_divisible'] = give_asset_info['divisible']
    elif(event['_category'] in ['dividends', 'sends',]):
        asset_info = mongo_db.tracked_assets.find_one({'asset': event['asset']})
        assert asset_info
        event['_divisible'] = asset_info['divisible']
    return event


#############
# Bitcoin-related

def normalize_amount(amount, divisible=True):
    if divisible:
        return float((D(amount) / D(config.UNIT)).quantize(D('.00000000'), rounding=decimal.ROUND_HALF_EVEN)) 
    else: return amount

def denormalize_amount(amount, divisible=True):
    if divisible:
        return int(amount * config.UNIT)
    else: return amount

def get_btc_supply(normalize=False):
    """returns the total supply of BTC (based on what bitcoind says the current block height is)"""
    block_count = config.CURRENT_BLOCK_INDEX
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
