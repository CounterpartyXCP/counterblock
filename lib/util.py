import os
import json
import base64
import logging
import requests

from . import (config,)

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
