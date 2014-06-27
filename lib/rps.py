from lib import config, util
from datetime import datetime
import logging
import decimal
import base64
import json

D = decimal.Decimal


def get_open_rps_count(possible_moves = 3, exclude_addresses = []):
    
    bindings = ['open', possible_moves]
    sql = 'SELECT wager, COUNT(*) AS game_count FROM rps WHERE status = ? AND possible_moves = ? '
    if isinstance(exclude_addresses, list) and len(exclude_addresses)>0:
        sql += 'AND source NOT IN ({}) '.format(','.join(['?' for e in range(0,len(exclude_addresses))]))
        bindings += exclude_addresses
    sql += 'GROUP BY wager ORDER BY tx_index DESC'
    
    params = {
        'query': sql,
        'bindings': bindings
    }   
  
    return util.call_jsonrpc_api('sql', params)['result']

def get_user_rps(addresses):

    games = []

    filters = [
        ('status', '=', 'open'),
        ('source', 'IN', addresses)
    ]
    rpss = util.call_jsonrpc_api('get_rps', {'filters': filters})['result']
    for rps in rpss:
        games.append({
            'block_index': rps['block_index'],
            'address': rps['source'],
            'tx_hash': rps['tx_hash'],
            'wager': rps['wager'],
            'move': 0,
            'counter_move': 0,
            'status': 'open',
            'possible_moves': rps['possible_moves']
        })

    filters = [
        ('tx0_address', 'IN', addresses),
        ('tx1_address', 'IN', addresses)
    ]
    valid_status = ['pending', 'resolved and pending', 'pending and resolved', 
                    'concluded: first player wins', 'concluded: second player wins', 'concluded: tie']
    params = {
        'filters': filters, 
        'filterop': 'OR', 
        'status':valid_status,
        'order_by': 'block_index',
        'order_dir': 'DESC'
    }

    rps_matches =  util.call_jsonrpc_api('get_rps_matches', params)['result']

    resolved_bindings = []
    match_games = {}

    for rps_match in rps_matches:

        if rps_match['status'] == 'concluded: tie':
            status = 'tie'
        elif rps_match['status'] in ['resolved and pending', 'pending and resolved']:
            status = 'resolved'
        else:
            status = 'pending'
        
        if rps_match['tx0_address'] in addresses:
            txn = 0
            if rps_match['status'] == 'concluded: first player wins':
                status = 'win'
            elif rps_match['status'] == 'concluded: second player wins':
                status = 'lose'     

        elif rps_match['tx1_address'] in addresses:
            txn = 1
            if rps_match['status'] == 'concluded: second player wins':
                status = 'win'
            elif rps_match['status'] == 'concluded: first player wins':
                status = 'lose'
        
        if status != 'pending':
            resolved_bindings.append(rps_match['id'])
        if status == 'resolved':
            status = 'pending'

        match_games[rps_match['id']] = {
            'block_index': rps_match['tx{}_block_index'.format(txn)],
            'address': rps_match['tx{}_address'.format(txn)],
            'tx_hash': rps_match['tx{}_hash'.format(txn)],
            'wager': rps_match['wager'],
            'move': 0,
            'counter_move': 0,
            'status': status,
            'possible_moves': rps_match['possible_moves']
        }

    if len(resolved_bindings) > 0:

        filters = [('rps_match_id', 'IN', resolved_bindings)]
        params = {
            'filters': filters, 
            'status': 'valid', 
            'order_by': 'block_index',
            'order_dir': 'DESC'
        }
        rpsresolves = util.call_jsonrpc_api('get_rpsresolves', params)['result']

        for rpsresolve in rpsresolves:
            rps_match_id = rpsresolve['rps_match_id']
            if match_games[rps_match_id]['address'] == rpsresolve['source']:
                match_games[rps_match_id]['move'] = rpsresolve['move']
            else:
                match_games[rps_match_id]['counter_move'] = rpsresolve['move']

    for match_games_key in match_games:
        games.append(match_games[match_games_key])

    return games
