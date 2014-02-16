import os
import json
import base64
import logging
import requests

from . import (config,)

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
    r = requests.post(
        endpoint, data=json.dumps(payload), headers={'content-type': 'application/json'}, auth=auth)
    if r.status_code != 200:
        raise Exception("Bad status code returned from counterwalletd: '%s'. payload: '%s'." % (r.status_code, r.text))
    
    result = r.json()
    if abort_on_error and 'error' in result:
        raise Exception("Got back error from server: %s" % result['error'])
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
