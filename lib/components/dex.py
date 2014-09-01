from datetime import datetime
import logging
import decimal
import base64
import json
import time

from lib import config, util

decimal.setcontext(decimal.Context(prec=8, rounding=decimal.ROUND_HALF_EVEN))
D = decimal.Decimal


def calculate_price(base_quantity, quote_quantity, base_divisibility, quote_divisibility, order_type = None):
    if not base_divisibility:
        base_quantity *= config.UNIT
    if not quote_divisibility:
        quote_quantity *= config.UNIT

    try: 
        if order_type == 'BUY':
            decimal.setcontext(decimal.Context(prec=8, rounding=decimal.ROUND_DOWN))
        elif order_type == 'SELL':
            decimal.setcontext(decimal.Context(prec=8, rounding=decimal.ROUND_UP))

        price = format(D(quote_quantity) / D(base_quantity), '.8f')

        decimal.setcontext(decimal.Context(prec=8, rounding=decimal.ROUND_HALF_EVEN))
        return price

    except Exception, e:
        decimal.setcontext(decimal.Context(prec=8, rounding=decimal.ROUND_HALF_EVEN))
        return '0'

def get_pairs_with_orders(addresses=[], max_pairs=12):

    pairs_with_orders = []

    sources = '''AND source IN ({})'''.format(','.join(['?' for e in range(0,len(addresses))]))
        
    sql = '''SELECT (MIN(give_asset, get_asset) || '/' || MAX(give_asset, get_asset)) AS pair,
                    COUNT(*) AS order_count
             FROM orders
             WHERE give_asset != get_asset AND status = ? {} 
             GROUP BY pair 
             ORDER BY order_count DESC
             LIMIT ?'''.format(sources)
    
    bindings = ['open'] + addresses + [max_pairs]

    my_pairs = util.call_jsonrpc_api('sql', {'query': sql, 'bindings': bindings})['result']

    for my_pair in my_pairs:
        base_asset, quote_asset = util.assets_to_asset_pair(*tuple(my_pair['pair'].split("/")))
        top_pair = {
            'base_asset': base_asset,
            'quote_asset': quote_asset,
            'my_order_count': my_pair['order_count']
        }
        if my_pair['pair'] == 'BTC/XCP': # XCP/BTC always in first
            pairs_with_orders.insert(0, top_pair)
        else:
            pairs_with_orders.append(top_pair)

    return pairs_with_orders


def get_pairs(quote_asset='XCP', exclude_pairs=[], max_pairs=12, from_time=None):
            
    bindings = []
    
    sql = '''SELECT (CASE
                        WHEN forward_asset = ? THEN backward_asset
                        ELSE forward_asset
                    END) AS base_asset,
                    (CASE
                        WHEN backward_asset = ? THEN backward_asset
                        ELSE forward_asset
                    END) AS quote_asset,
                    (CASE
                        WHEN backward_asset = ? THEN (forward_asset || '/' || backward_asset)
                        ELSE (backward_asset || '/' || forward_asset)
                    END) AS pair,
                    (CASE
                        WHEN forward_asset = ? THEN backward_quantity
                        ELSE forward_quantity
                    END) AS bq,
                    (CASE
                        WHEN backward_asset = ? THEN backward_quantity
                        ELSE forward_quantity
                    END) AS qq '''
    if from_time:
        sql += ''', block_time '''

    sql += '''FROM order_matches '''
    bindings += [quote_asset, quote_asset, quote_asset, quote_asset, quote_asset]

    if from_time:
        sql += '''INNER JOIN blocks ON order_matches.block_index = blocks.block_index '''

    priority_quote_assets = []
    for priority_quote_asset in config.QUOTE_ASSETS:
        if priority_quote_asset != quote_asset:
            priority_quote_assets.append(priority_quote_asset)
        else:
            break

    if len(priority_quote_assets) > 0:
        asset_bindings = ','.join(['?' for e in range(0,len(priority_quote_assets))])
        sql += '''WHERE ((forward_asset = ? AND backward_asset NOT IN ({})) 
                         OR (forward_asset NOT IN ({}) AND backward_asset = ?)) '''.format(asset_bindings, asset_bindings)
        bindings += [quote_asset] + priority_quote_assets + priority_quote_assets + [quote_asset]
    else:
        sql += '''WHERE ((forward_asset = ?) OR (backward_asset = ?)) '''
        bindings += [quote_asset, quote_asset]

    if len(exclude_pairs) > 0:
        sql += '''AND pair NOT IN ({}) '''.format(','.join(['?' for e in range(0,len(exclude_pairs))]))
        bindings += exclude_pairs

    if from_time:
        sql += '''AND block_time > ? '''
        bindings += [from_time]

    sql += '''AND forward_asset != backward_asset
              AND status = ?'''

    bindings += ['completed', max_pairs]

    sql = '''SELECT base_asset, quote_asset, pair, SUM(bq) AS base_quantity, SUM(qq) AS quote_quantity 
             FROM ({}) 
             GROUP BY pair 
             ORDER BY quote_quantity DESC
             LIMIT ?'''.format(sql)

    return util.call_jsonrpc_api('sql', {'query': sql, 'bindings': bindings})['result']


def get_quotation_pairs(exclude_pairs=[], max_pairs=12, from_time=None):

    all_pairs = []

    for currency in config.MARKET_LIST_QUOTE_ASSETS:
        currency_pairs = get_pairs(quote_asset=currency, exclude_pairs=exclude_pairs, max_pairs=max_pairs, from_time=from_time)
        max_pairs = max_pairs - len(currency_pairs)
        for currency_pair in currency_pairs:
            if currency_pair['pair'] == 'XCP/BTC':
                all_pairs.insert(0, currency_pair)
            else:
                all_pairs.append(currency_pair)

    return all_pairs

@util.block_cache
def get_users_pairs(addresses=[], max_pairs=12):
    
    top_pairs = []
    all_assets = []
    exclude_pairs = []

    if len(addresses) > 0:
        top_pairs += get_pairs_with_orders(addresses, max_pairs)

    for p in top_pairs:
        exclude_pairs += [p['base_asset'] + '/' + p['quote_asset']]
        all_assets += [p['base_asset'], p['quote_asset']]

    for currency in config.MARKET_LIST_QUOTE_ASSETS:
        if len(top_pairs) < max_pairs:
            limit = max_pairs - len(top_pairs)
            currency_pairs = get_pairs(currency, exclude_pairs, limit)
            for currency_pair in currency_pairs:
                top_pair = {
                    'base_asset': currency_pair['base_asset'],
                    'quote_asset': currency_pair['quote_asset']
                }
                if currency_pair['pair'] == 'XCP/BTC': # XCP/BTC always in first
                    top_pairs.insert(0, top_pair)
                else:
                    top_pairs.append(top_pair)
                all_assets += [currency_pair['base_asset'], currency_pair['quote_asset']]

    if 'XCP/BTC' not in [p['base_asset'] + '/' + p['quote_asset'] for p in top_pairs]:
        top_pairs.insert(0, {
            'base_asset': 'XCP',
            'quote_asset': 'BTC'
        })
        all_assets += ['XCP', 'BTC']

    top_pairs = top_pairs[:12]
    all_assets = list(set(all_assets))
    supplies = get_assets_supply(all_assets)

    for p in range(len(top_pairs)):
        price, trend, price24h, progression = get_price_movement(top_pairs[p]['base_asset'], top_pairs[p]['quote_asset'], supplies=supplies)
        top_pairs[p]['price'] = format(price, ".8f")
        top_pairs[p]['trend'] = trend
        top_pairs[p]['progression'] = format(progression, ".2f")
        top_pairs[p]['price_24h'] = format(price24h, ".8f")

    return top_pairs

def merge_same_price_orders(orders):   
    if len(orders) > 1:
        merged_orders = []
        orders = sorted(orders, key=lambda x: float(x['price']))
        merged_orders.append(orders[0])
        for o in range(1, len(orders)):
            if float(orders[o]['price']) == float(merged_orders[-1]['price']):
                merged_orders[-1]['amount'] += orders[o]['amount']
                merged_orders[-1]['total'] += orders[o]['total']
            else:
                merged_orders.append(orders[o])
        return merged_orders
    else:
        return orders

@util.block_cache
def get_market_orders(asset1, asset2, addresses=[], supplies=None, min_fee_provided=0.95, max_fee_required=0.95):

    base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
    if not supplies:
        supplies = get_assets_supply([asset1, asset2])

    market_orders = []
    buy_orders = []
    sell_orders = []

    sql = '''SELECT orders.*, blocks.block_time FROM orders INNER JOIN blocks ON orders.block_index=blocks.block_index 
             WHERE  status = ? '''
    bindings = ['open']

    if len(addresses) > 0:
        sql += '''AND source IN ({}) '''.format(','.join(['?' for e in range(0,len(addresses))]))
        bindings += addresses

    sql += '''AND give_remaining > 0 
              AND give_asset IN (?, ?) 
              AND get_asset IN (?, ?) 
              ORDER BY tx_index DESC'''

    bindings +=  [asset1, asset2, asset1, asset2]

    orders = util.call_jsonrpc_api('sql', {'query': sql, 'bindings': bindings})['result']

    for order in orders:
        market_order = {}

        exclude = False
        if order['give_asset'] == 'BTC':
            try:
                fee_provided = order['fee_provided'] / (order['give_quantity'] / 100)
                market_order['fee_provided'] = format(D(order['fee_provided']) / (D(order['give_quantity']) / D(100)), '.2f') 
            except Exception, e:
                fee_provided = min_fee_provided - 1 # exclude
            
            exclude = fee_provided < min_fee_provided

        elif order['get_asset'] == 'BTC':
            try:
                fee_required = order['fee_required'] / (order['get_quantity'] / 100)
                market_order['fee_required'] = format(D(order['fee_required']) / (D(order['get_quantity']) / D(100)), '.2f')
            except Exception, e:
                fee_required = max_fee_required + 1 # exclude    

            exclude = fee_required > max_fee_required
 
        
        if not exclude:
            if order['give_asset'] == base_asset:
                price = calculate_price(order['give_quantity'], order['get_quantity'], supplies[order['give_asset']][1], supplies[order['get_asset']][1], 'SELL')
                market_order['type'] = 'SELL'
                market_order['amount'] = order['give_remaining']
                market_order['total'] = D(order['give_remaining']) * D(price)
                if not supplies[order['give_asset']][1] and supplies[order['get_asset']][1]:
                    market_order['total'] = int(market_order['total'] * config.UNIT)
                elif supplies[order['give_asset']][1] and not supplies[order['get_asset']][1]:
                    market_order['total'] = int(market_order['total'] / config.UNIT)
                else:
                    market_order['total'] = int(market_order['total'])
            else:
                price = calculate_price(order['get_quantity'], order['give_quantity'], supplies[order['get_asset']][1], supplies[order['give_asset']][1], 'BUY')
                market_order['type'] = 'BUY'
                market_order['total'] = order['give_remaining']
                market_order['amount'] = D(order['give_remaining']) / D(price)
                if supplies[order['give_asset']][1] and not supplies[order['get_asset']][1]:
                    market_order['amount'] = int(market_order['amount'] / config.UNIT)
                elif not supplies[order['give_asset']][1] and supplies[order['get_asset']][1]:
                    market_order['amount'] = int(market_order['amount'] * config.UNIT)
                else:
                    market_order['amount'] = int(market_order['amount'])

            market_order['price'] = price

            if len(addresses) > 0:
                completed = format(((D(order['give_quantity']) - D(order['give_remaining'])) / D(order['give_quantity'])) * D(100), '.2f') 
                market_order['completion'] = "{}%".format(completed)
                market_order['tx_index'] = order['tx_index']
                market_order['tx_hash'] = order['tx_hash']
                market_order['source'] = order['source']
                market_order['block_index'] = order['block_index']
                market_order['block_time'] = order['block_time']
                market_orders.append(market_order)
            else:
                if market_order['type'] == 'SELL':
                    sell_orders.append(market_order)
                else:
                    buy_orders.append(market_order)

    if len(addresses) == 0:
        market_orders = merge_same_price_orders(sell_orders) + merge_same_price_orders(buy_orders)

    return market_orders

@util.block_cache
def get_market_trades(asset1, asset2, addresses=[], limit=100, supplies=None):

    base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
    if not supplies:
        supplies = get_assets_supply([asset1, asset2])
    market_trades = []

    sources = ''
    bindings = ['expired']
    if len(addresses) > 0:
        placeholder = ','.join(['?' for e in range(0,len(addresses))])
        sources = '''AND (tx0_address IN ({}) OR tx1_address IN ({}))'''.format(placeholder, placeholder)
        bindings += addresses + addresses

    sql = '''SELECT order_matches.*, blocks.block_time FROM order_matches INNER JOIN blocks ON order_matches.block_index=blocks.block_index
             WHERE status != ? {}
                AND forward_asset IN (?, ?) 
                AND backward_asset IN (?, ?) 
             ORDER BY block_index DESC'''.format(sources)

    bindings +=  [asset1, asset2, asset1, asset2]

    order_matches = util.call_jsonrpc_api('sql', {'query': sql, 'bindings': bindings})['result']

    for order_match in order_matches:

        if order_match['tx0_address'] in addresses:
            trade = {}
            trade['match_id'] = order_match['id']
            trade['source'] = order_match['tx0_address']
            trade['countersource'] = order_match['tx1_address']
            trade['block_index'] = order_match['block_index']
            trade['block_time'] = order_match['block_time']
            trade['status'] = order_match['status']
            if order_match['forward_asset'] == base_asset:
                trade['type'] = 'SELL'
                trade['price'] = calculate_price(order_match['forward_quantity'], order_match['backward_quantity'], supplies[order_match['forward_asset']][1], supplies[order_match['backward_asset']][1], 'SELL')
                trade['amount'] = order_match['forward_quantity']
                trade['total'] = order_match['backward_quantity']
            else:
                trade['type'] = 'BUY'
                trade['price'] = calculate_price(order_match['backward_quantity'], order_match['forward_quantity'], supplies[order_match['backward_asset']][1], supplies[order_match['forward_asset']][1], 'BUY')
                trade['amount'] = order_match['backward_quantity']
                trade['total'] = order_match['forward_quantity']
            market_trades.append(trade)

        if len(addresses)==0 or order_match['tx1_address'] in addresses:
            trade = {}
            trade['match_id'] = order_match['id']
            trade['source'] = order_match['tx1_address']
            trade['countersource'] = order_match['tx0_address']
            trade['block_index'] = order_match['block_index']
            trade['block_time'] = order_match['block_time']
            trade['status'] = order_match['status']
            if order_match['backward_asset'] == base_asset:
                trade['type'] = 'SELL'
                trade['price'] = calculate_price(order_match['backward_quantity'], order_match['forward_quantity'], supplies[order_match['backward_asset']][1], supplies[order_match['forward_asset']][1], 'SELL')
                trade['amount'] = order_match['backward_quantity']
                trade['total'] = order_match['forward_quantity']
            else:
                trade['type'] = 'BUY'
                trade['price'] = calculate_price(order_match['forward_quantity'], order_match['backward_quantity'], supplies[order_match['forward_asset']][1], supplies[order_match['backward_asset']][1], 'BUY')
                trade['amount'] = order_match['forward_quantity']
                trade['total'] = order_match['backward_quantity']
            market_trades.append(trade)

    return market_trades


def get_assets_supply(assets=[]):

    supplies = {}

    if 'XCP' in assets:
        supplies['XCP'] = (util.call_jsonrpc_api('get_xcp_supply', [])['result'], True)
        assets.remove('XCP')

    if 'BTC' in assets:
        supplies['BTC'] = (0, True)
        assets.remove('BTC')

    if len(assets) > 0:
        sql = '''SELECT asset, SUM(quantity) AS supply, divisible FROM issuances 
                 WHERE asset IN ({}) 
                 AND status = ?
                 GROUP BY asset
                 ORDER BY asset'''.format(','.join(['?' for e in range(0,len(assets))]))
        bindings = assets + ['valid']

        issuances = util.call_jsonrpc_api('sql', {'query': sql, 'bindings': bindings})['result']
        for issuance in issuances:
            supplies[issuance['asset']] = (issuance['supply'], issuance['divisible'])

    return supplies


def get_pair_price(base_asset, quote_asset, max_block_time=None, supplies=None):

    if not supplies:
        supplies = get_assets_supply([base_asset, quote_asset])

    sql = '''SELECT *, MAX(tx0_index, tx1_index) AS tx_index, blocks.block_time 
             FROM order_matches INNER JOIN blocks ON order_matches.block_index = blocks.block_index
             WHERE 
                forward_asset IN (?, ?) AND
                backward_asset IN (?, ?) '''
    bindings = [base_asset, quote_asset, base_asset, quote_asset]

    if max_block_time:
        sql += '''AND block_time <= ? '''
        bindings += [max_block_time]

    sql += '''ORDER BY tx_index DESC
             LIMIT 2'''
    
    order_matches = util.call_jsonrpc_api('sql', {'query': sql, 'bindings': bindings})['result']

    if len(order_matches) == 0:
        last_price = D(0.0)
    elif order_matches[0]['forward_asset'] == base_asset:
        last_price = calculate_price(order_matches[0]['forward_quantity'], order_matches[0]['backward_quantity'], supplies[order_matches[0]['forward_asset']][1], supplies[order_matches[0]['backward_asset']][1])
    else:
        last_price = calculate_price(order_matches[0]['backward_quantity'], order_matches[0]['forward_quantity'], supplies[order_matches[0]['backward_asset']][1], supplies[order_matches[0]['forward_asset']][1])

    trend = 0
    if len(order_matches) == 2:
        if order_matches[1]['forward_asset'] == base_asset:
            before_last_price = calculate_price(order_matches[0]['forward_quantity'], order_matches[0]['backward_quantity'], supplies[order_matches[0]['forward_asset']][1], supplies[order_matches[0]['backward_asset']][1])
        else:
            before_last_price = calculate_price(order_matches[0]['backward_quantity'], order_matches[0]['forward_quantity'], supplies[order_matches[0]['backward_asset']][1], supplies[order_matches[0]['forward_asset']][1])
        if last_price < before_last_price:
            trend = -1
        elif last_price > before_last_price:
            trend = 1

    return D(last_price), trend

def get_price_movement(base_asset, quote_asset, supplies=None):

    yesterday = int(time.time() - (24*60*60))
    if not supplies:
        supplies = get_assets_supply([base_asset, quote_asset])

    price, trend = get_pair_price(base_asset, quote_asset, supplies=supplies)
    price24h, trend24h = get_pair_price(base_asset, quote_asset, max_block_time=yesterday, supplies=supplies)
    try:
        progression = (price - price24h) / (price24h / D(100))
    except:
        progression = D(0)

    return price, trend, price24h, progression

@util.block_cache
def get_markets_list(mongo_db=None):
    
    yesterday = int(time.time() - (24*60*60))
    markets = []
    pairs = []

    # pairs with volume last 24h
    pairs += get_quotation_pairs(exclude_pairs=[], max_pairs=500, from_time=yesterday)
    pair_with_volume = [p['pair'] for p in pairs]

    # pairs without volume last 24h
    pairs += get_quotation_pairs(exclude_pairs=pair_with_volume, max_pairs=500 - len(pair_with_volume))

    base_assets  = [p['base_asset'] for p in pairs]
    quote_assets  = [p['quote_asset'] for p in pairs]
    all_assets = list(set(base_assets + quote_assets))
    supplies = get_assets_supply(all_assets)

    asset_with_image = {}
    if mongo_db:
        infos = mongo_db.asset_extended_info.find({'asset': {'$in': all_assets}}, {'_id': 0}) or False
        for info in infos:
            if 'info_data' in info and 'valid_image' in info['info_data'] and info['info_data']['valid_image']:
                asset_with_image[info['asset']] = True
    
    for pair in pairs:
        price, trend, price24h, progression = get_price_movement(pair['base_asset'], pair['quote_asset'], supplies=supplies)
        market = {}
        market['base_asset'] = pair['base_asset']
        market['quote_asset'] = pair['quote_asset']
        market['volume'] = pair['quote_quantity'] if pair['pair'] in pair_with_volume else 0
        market['price'] = format(price, ".8f")
        market['trend'] = trend
        market['progression'] = format(progression, ".2f")
        market['price_24h'] = format(price24h, ".8f")
        market['supply'] = supplies[pair['base_asset']][0]
        market['base_divisibility'] = supplies[pair['base_asset']][1]
        market['quote_divisibility'] = supplies[pair['quote_asset']][1]
        market['market_cap'] = format(D(market['supply']) * D(market['price']), ".4f")
        market['with_image'] = True if pair['base_asset'] in asset_with_image else False
        if market['base_asset'] == 'XCP' and market['quote_asset'] == 'BTC':
            markets.insert(0, market)
        else:
            markets.append(market)

    for m in range(len(markets)):
        markets[m]['pos'] = m + 1

    return markets

@util.block_cache
def get_market_details(asset1, asset2, min_fee_provided=0.95, max_fee_required=0.95, mongo_db=None):

    yesterday = int(time.time() - (24*60*60))
    base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)

    supplies = get_assets_supply([base_asset, quote_asset])
    
    price, trend, price24h, progression = get_price_movement(base_asset, quote_asset, supplies=supplies)

    buy_orders = []
    sell_orders = []
    market_orders = get_market_orders(base_asset, quote_asset, supplies=supplies, min_fee_provided=min_fee_provided, max_fee_required=max_fee_required)
    for order in market_orders:
        if order['type'] == 'SELL':
            sell_orders.append(order)
        elif order['type'] == 'BUY':
            buy_orders.append(order)

    last_trades =  get_market_trades(base_asset, quote_asset, supplies=supplies)

    ext_info = False
    if mongo_db:
        ext_info = mongo_db.asset_extended_info.find_one({'asset': base_asset}, {'_id': 0})
        if ext_info and 'info_data' in ext_info:
            ext_info = ext_info['info_data']
        else:
            ext_info = False

    return {
        'base_asset': base_asset,
        'quote_asset': quote_asset,
        'price': format(price, ".8f"),
        'trend': trend,
        'progression': format(progression, ".2f"),
        'price_24h': format(price24h, ".8f"),
        'supply': supplies[base_asset][0],
        'base_asset_divisible': supplies[base_asset][1],
        'quote_asset_divisible': supplies[quote_asset][1],
        'buy_orders': sorted(buy_orders, key=lambda x: x['price'], reverse=True),
        'sell_orders': sorted(sell_orders, key=lambda x: x['price']),
        'last_trades': last_trades,
        'base_asset_infos': ext_info
    }




