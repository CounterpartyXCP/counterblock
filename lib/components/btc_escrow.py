import os
import logging
import decimal
import base64
import json
import math
from datetime import datetime
import time

from lib import config, util, util_bitcoin, blockchain

# TODO: use deterministic wallet for the 5 following functions
def get_new_address(order_tx_hash):
    new_address = util.bitcoind_rpc('getnewaddress', ['',])
    return new_address


def check_signed_hash(order_source, order_tx_hash, order_signed_tx_hash):
    # disabled until Bitcore fix message signature
    # verify_result = util.bitcoind_rpc('verifymessage', [order_source, order_signed_tx_hash, order_tx_hash]) 
    # return verify_result == 'true'
    return True


def get_escrow_balance(escrow_address, min_conf = 0):
    wallet_unspent = util.bitcoind_rpc('listunspent', [min_conf, 999999])
    escrow_balance = sum([output['amount'] for output in wallet_unspent if output['address'] == escrow_address])
    return util_bitcoin.denormalize_quantity(escrow_balance)


def get_escrow_unspent(escrow_address, min_conf = 0):
    wallet_unspent = util.bitcoind_rpc('listunspent', [min_conf, 999999])
    return [{'txid': output['txid'], 'vout': output['vout']} for output in wallet_unspent if output['address'] == escrow_address]


def send_to_many(inputs, destinations, satoshi = False):
    if satoshi:
        for destination in destinations:
            destinations[destination] = util_bitcoin.normalize_quantity(destinations[destination])

    unsigned_raw_transaction = util.bitcoind_rpc('createrawtransaction', [inputs, destinations])
    signed_raw_transaction = util.bitcoind_rpc('signrawtransaction', [unsigned_raw_transaction]);
    if 'complete' in signed_raw_transaction and signed_raw_transaction['complete'] == 1:
        return util.bitcoind_rpc('sendrawtransaction', [signed_raw_transaction['hex']])
    else:
        return None


def find_or_create_escrow_info(db, order_source, order_tx_hash, order_signed_tx_hash, wallet_id):
    escrow_info = db.escrow_infos.find_one({'order_tx_hash': order_tx_hash})
    if escrow_info:
        if escrow_info['order_source'] == order_source and \
           escrow_info['order_signed_tx_hash'] == order_signed_tx_hash and \
           escrow_info['wallet_id'] == wallet_id: 
            return escrow_info
        else:
            raise Exception("Invalid signature for transaction hash %s" % order_tx_hash)
    else:
        if check_signed_hash(order_source, order_tx_hash, order_signed_tx_hash):
            escrow_info = {
                'wallet_id': wallet_id,
                'order_source': order_source,
                'order_tx_hash': order_tx_hash,
                'order_signed_tx_hash': order_signed_tx_hash,
                'created_at': time.time(),
                'escrow_address': get_new_address(order_tx_hash),
                'status': 'open', # open, expired, canceled
                'need_refund': False,
                'refunded': False, # use to get escrowed balance
                'payments': []
            }
            escrow_info['_id'] = db.escrow_infos.insert(escrow_info)
            escrow_info['_id'] = str(escrow_info['_id'])
            return escrow_info
        else:
            raise Exception("Invalid signature for transaction hash %s" % order_tx_hash)

def get_escrowed_balances(db, wallet_id):
    escrow_infos = db.escrow_infos.find({'wallet_id': wallet_id, 'refunded': False}, {'_id': 0})
    results = {}
    for escrow_info in escrow_infos:
        escrowed_balance =  get_escrow_balance(escrow_info['escrow_address'])
        if (escrowed_balance > 0):
            if escrow_info['order_source'] in results:
                results[escrow_info['order_source']] += escrowed_balance
            else:
                results[escrow_info['order_source']] = escrowed_balance
    return results

def get_by_order_signed_tx_hashes(db, order_signed_tx_hashes, status='open'):
    records = db.escrow_infos.find({'order_signed_tx_hash': {'$in': order_signed_tx_hashes}, 'status': status}, {'_id': 0})
    result = []
    for r in records:
        result.append(r)
    return result


def make_btcpay(db, order_match, escrow_info):

    for payment in escrow_info['payments']:
        if payment['order_match_id'] == order_match['id']:
            return

    order_match_quantity  = order_match['forward_quantity'] if order_match['forward_asset'] == 'BTC' else order_match['backward_quantity']
    commission = int(config.ESCROW_COMMISSION * order_match_quantity)
    previous_commission = sum([p['commission'] for p in escrow_info['payments'] if not p['error']]) # don't use commission for previous order matches
    required_escrow_amount  = order_match_quantity + config.MIN_FEE + commission + previous_commission
    available_escrow_amount = get_escrow_balance(escrow_info['escrow_address'], min_conf = config.MIN_CONF_FOR_ESCROWED_FUND)

    if available_escrow_amount >= required_escrow_amount:
        btcpay_params = {
            'source': escrow_info['escrow_address'],                
            'order_match_id': order_match['id'],
            'allow_unconfirmed_inputs': True
        }
        payment = {
            'order_match_id': order_match['id']
        }
        try:
            btcpay_tx_hash = util.call_jsonrpc_api("do_btcpay", btcpay_params)
            payment['btcpay_tx_hash'] = btcpay_tx_hash['result']
            payment['commission'] = commission
            payment['error'] = None
            message = 'BTCPay done for order_match {} with escrow address {}. BTCPay tx hash is: {}'
            logging.info(message.format(order_match['id'], escrow_info['escrow_address'], btcpay_tx_hash['result']))
        except Exception, e:
            logging.exception(e)
            payment['error'] = str(e)
        
        escrow_info['payments'].append(payment)
        db.escrow_infos.save(escrow_info)
    else:
        message = 'Insufficient fund in escrow address to proceed BTCPay for order_match {}. Available amount in {} : {}. Required amount : {}'
        logging.info(message.format(order_match['id'], escrow_info['escrow_address'], available_escrow_amount, required_escrow_amount))

    return escrow_info


def close_escrowed_order(db, escrow_info, status):
    escrow_info['status'] = status
    escrow_info['need_refund'] = get_escrow_balance(escrow_info['escrow_address']) > 0
    db.escrow_infos.save(escrow_info)


def get_escrow_infos(db, filters):
    escrow_infos = db.escrow_infos.find(filters)
    orders_hashes = []
    escrow_info_by_tx_hash = {}
    for escrow_info in escrow_infos:
        orders_hashes.append(escrow_info['order_tx_hash'])
        escrow_info_by_tx_hash[escrow_info['order_tx_hash']] = escrow_info
    return orders_hashes, escrow_info_by_tx_hash


def get_open_escrow_infos(db):
    return get_escrow_infos(db, {'status': 'open'})


def get_payable_order_matches(escrowed_order_hashes, current_block_index):
    tx_hashes_bind = ','.join(['?' for e in range(0,len(escrowed_order_hashes))])
    sql  = '''SELECT * FROM order_matches WHERE '''
    sql += '''status = ? AND '''
    sql += '''(forward_asset = ? OR backward_asset = ?) AND '''
    sql += '''(tx0_hash IN ({}) OR tx1_hash IN ({})) AND '''.format(tx_hashes_bind, tx_hashes_bind)
    sql += '''block_index = ? '''
    bindings = ['pending', 'BTC', 'BTC'] + escrowed_order_hashes + escrowed_order_hashes + [current_block_index - config.AUTOBTCESCROW_NUM_BLOCKS_FOR_BTCPAY]
    return util.call_jsonrpc_api('sql', {'query': sql, 'bindings': bindings})['result']


def cancel_escrowed_orders(db):
    orders_hashes, escrow_info_by_tx_hash = get_open_escrow_infos(db)
    if len(orders_hashes) > 0:
        filters = [('offer_hash', 'IN', orders_hashes)]
        canceled_orders = util.call_jsonrpc_api('get_cancels', {'filters': filters})['result']
        for canceled_order in canceled_orders:
            close_escrowed_order(db, escrow_info_by_tx_hash[canceled_order['offer_hash']], 'canceled')


def expire_escrowed_orders(db):
    orders_hashes, escrow_info_by_tx_hash = get_open_escrow_infos(db)
    if len(orders_hashes) > 0:
        filters = [('order_hash', 'IN', orders_hashes)]
        expired_orders = util.call_jsonrpc_api('get_order_expirations', {'filters': filters})['result']
        for expired_order in expired_orders:
            close_escrowed_order(db, escrow_info_by_tx_hash[expired_order['order_hash']], 'expired')


def pay_escrowed_orders(db, current_block_index):
    orders_hashes, escrow_info_by_tx_hash = get_open_escrow_infos(db)
    if len(orders_hashes) > 0:
        order_matches = get_payable_order_matches(orders_hashes, current_block_index)
        for order_match in order_matches:
            order_tx_hash = order_match['tx0_hash'] if order_match['forward_asset'] == 'BTC' else order_match['tx1_hash']
            escrow_info_by_tx_hash[order_tx_hash] = make_btcpay(db, order_match, escrow_info_by_tx_hash[order_tx_hash])


def refund_escrowed_orders(db):
    
    commission_address = config.ESCROW_COMMISSION_ADDRESS
    orders_hashes, escrow_info_by_tx_hash = get_escrow_infos(db, {'need_refund': True})
    if len(orders_hashes) == 0:
        return
    
    inputs = []
    destinations = {
        commission_address: 0
    }
    total_in = 0
    total_out = 0

    for order_hash in escrow_info_by_tx_hash:
        escrow_info = escrow_info_by_tx_hash[order_hash]

        inputs += get_escrow_unspent(escrow_info['escrow_address'])

        refund_address = escrow_info['order_source']
        if refund_address not in destinations:
            destinations[refund_address] = 0

        escrowed_amount = get_escrow_balance(escrow_info['escrow_address'])
        total_in += escrowed_amount
        commission = sum([p['commission'] for p in escrow_info['payments'] if not p['error']])
        refund_amount = escrowed_amount - commission
        
        if refund_amount >= config.REGULAR_DUST_SIZE:
            destinations[refund_address] += refund_amount
            total_out += refund_amount
        
        destinations[commission_address] += commission
        total_out += commission

    if total_out > config.REGULAR_DUST_SIZE:
        change_quantity = total_out - total_in # == sum of dust amount

        # fees
        ouput_size = 34 * len(destinations.keys())
        input_size = 181 * len(inputs)
        transaction_size = input_size + ouput_size + 10
        necessary_fee = (int(transaction_size / 1000) + 1) * config.DEFAULT_FEE_PER_KB
        missing_fee = necessary_fee - change_quantity

        if missing_fee > 0:
            # if commission is enought to pay fee
            if commission >= missing_fee + config.REGULAR_DUST_SIZE:
                destinations[commission_address] -= missing_fee
                missing_fee = 0
            # else we share between users what it's missing
            else:
                missing_fee -= destinations[commission_address]
                del(destinations[commission_address])
                destination_count = len([address for address in destinations if destinations[address] > config.REGULAR_DUST_SIZE])
                fee_by_address = int(math.ceil(missing_fee / destination_count))
                while missing_fee > 0 and destination_count > 0:
                    for address in destinations:
                        if destinations[address] > config.REGULAR_DUST_SIZE + fee_by_address:
                            destinations[address] -= fee_by_address
                            missing_fee -= fee_by_address
                    destination_count = len([address for address in destinations if destinations[address] > config.REGULAR_DUST_SIZE])
                    fee_by_address = int(math.ceil(missing_fee / destination_count))

        if missing_fee <= 0:
            refund_error = None
            try:
                refund_tx_hash = send_to_many(inputs, destinations, satoshi=True)
                logging.info('Refund done. Inuputs: {}. Destinations: {}. Tx hash: {}'.format(inputs, destinations, refund_tx_hash))
            except Exception, e:
                logging.exception(e)
                refund_error = str(e)

            for order_hash in escrow_info_by_tx_hash:
                escrow_info = escrow_info_by_tx_hash[order_hash]
                if not refund_error:
                    escrow_info['refund_tx_hash'] = refund_tx_hash
                    escrow_info['refunded'] = True
                else:
                    escrow_info['refund_error'] = refund_error
                escrow_info['need_refund'] = False
                db.escrow_infos.save(escrow_info)


def process_new_block(db, current_block_index):
    if config.AUTO_BTC_ESCROW_MACHINE:
        cancel_escrowed_orders(db)
        expire_escrowed_orders(db)
        pay_escrowed_orders(db, current_block_index)
        refund_escrowed_orders(db)

