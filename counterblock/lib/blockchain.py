import os
import re
import logging
import binascii
import hashlib
import json
import datetime
import decimal

from repoze.lru import lru_cache
from pycoin import encoding

from counterblock.lib import config, util

D = decimal.Decimal
decimal.getcontext().prec = 8
logger = logging.getLogger(__name__)


def round_out(num):
    """round out to 8 decimal places"""
    return float(D(num))


def normalize_quantity(quantity, divisible=True):
    """Goes from satoshis to normal human readable format"""
    if divisible:
        return float((D(quantity) / D(config.UNIT)))
    else:
        return quantity


def denormalize_quantity(quantity, divisible=True):
    """Goes from normal human readable format to satoshis"""
    if divisible:
        return int(quantity * config.UNIT)
    else:
        return quantity


def get_btc_supply(normalize=False, at_block_index=None):
    """returns the total supply of BTC (based on what bitcoind says the current block height is)"""
    block_count = config.state['my_latest_block']['block_index'] if at_block_index is None else at_block_index
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


def pubkey_to_address(pubkey_hex):
    sec = binascii.unhexlify(pubkey_hex)
    compressed = encoding.is_sec_compressed(sec)
    public_pair = encoding.sec_to_public_pair(sec)
    address_prefix = b'\x6f' if (config.TESTNET or config.REGTEST) else b'\x00'
    return encoding.public_pair_to_bitcoin_address(public_pair, compressed=compressed, address_prefix=address_prefix)


def bitcoind_rpc(command, params):
    return util.call_jsonrpc_api(
        command,
        params=params,
        endpoint=config.BACKEND_URL_NOAUTH,
        auth=config.BACKEND_AUTH,
        abort_on_error=True)['result']


def is_multisig(address):
    array = address.split('_')
    return (len(array) > 1)


def get_btc_balance(address, confirmed=True):
    all_unspent, confirmed_unspent = get_unspent_txouts(address, return_confirmed=True)
    unspent = confirmed_unspent if confirmed else all_unspent
    return sum(out['amount'] for out in unspent)


def listunspent(address):
    outputs = get_unspent_txouts(address)
    utxo = []
    for txo in outputs:
        newtxo = {
            'address': address,
            'txid': txo['txid'],
            'vout': txo['vout'],
            'ts': 0,
            'amount': str(txo['amount']),
            'confirmations': txo['confirmations'],
            'confirmationsFromCache': False
        }
        utxo.append(newtxo)
    return utxo


def getaddressinfo(address):
    all_unspent, confirmed_unspent = get_unspent_txouts(address, return_confirmed=True)
    balance = sum(out['amount'] for out in confirmed_unspent)
    unconfirmed_balance = sum(out['amount'] for out in all_unspent) - balance

    if is_multisig(address):
        array = address.split('_')
        # TODO: filter transactions
        #raw_transactions = reversed(search_raw_transactions(array[1:-1][1]))
        raw_transactions = search_raw_transactions(array[1:-1][1])
    else:
        #raw_transactions = reversed(search_raw_transactions(address))
        raw_transactions = search_raw_transactions(address)

    # TODO: This code was disabling counterwallet and it really doesn't has reason to exist
    # if someone is reading this and wondering why this comment is here, it's because i'm
    # blindly commenting it out because it worked in production, but don't understand if
    # there's a far reaching implication of it
    #try:
    #    raw_transactions = reversed(raw_transactions)
    #except Exception as e:
    #    raw_transactions = {}

    transactions = []
    for tx in raw_transactions:
        if 'confirmations' in tx and tx['confirmations'] > 0:
            transactions.append(tx['txid'])

    return {
        'addrStr': address,
        'balance': balance,
        'balanceSat': str(balance * config.UNIT),
        'unconfirmedBalance': str(unconfirmed_balance),
        'unconfirmedBalanceSat': str(unconfirmed_balance * config.UNIT),
        'transactions': transactions
    }


def gettransaction_batch(txhash_list):
    raw_txes = util.call_jsonrpc_api("getrawtransaction_batch", {
        'txhash_list': txhash_list,
        'verbose': True,
        'skip_missing': True}, abort_on_error=True)['result']
    txes = {}

    for tx_hash, tx in raw_txes.items():
        if tx is None:
            txes[tx_hash] = None
            continue

        valueOut = 0
        for vout in tx['vout']:
            valueOut += vout['value']
        txes[tx_hash] = {
            'txid': tx_hash,
            'version': tx['version'],
            'locktime': tx['locktime'],
            'confirmations': tx['confirmations'] if 'confirmations' in tx else 0,
            'blocktime': tx['blocktime'] if 'blocktime' in tx else 0,
            'blockhash': tx['blockhash'] if 'blockhash' in tx else 0,
            'time': tx['time'] if 'time' in tx else 0,
            'valueOut': valueOut,
            'vin': tx['vin'],
            'vout': tx['vout']
        }
    return txes


def gettransaction(tx_hash):
    return gettransaction_batch([tx_hash, ])[tx_hash]


def get_pubkey_from_transactions(address, raw_transactions):
    # for each transaction we got back, extract the vin, pubkey, go through, convert it to binary, and see if it reduces down to the given address
    for tx in raw_transactions:
        # parse the pubkey out of the first sent transaction
        for vin in tx['vin']:
            scriptsig = vin['scriptSig']
            asm = scriptsig['asm'].split(' ')
            pubkey_hex = asm[1]
            try:
                if pubkey_to_address(pubkey_hex) == address:
                    return pubkey_hex
            except:
                pass
    return None


def get_pubkey_for_address(address):
    if is_multisig(address):
        array = address.split('_')
        addresses = array[1:-1]
    else:
        addresses = [address]

    pubkeys = []

    for address in addresses:
        raw_transactions = search_raw_transactions(address)
        pubkey = get_pubkey_from_transactions(address, raw_transactions)
        if pubkey:
            pubkeys.append(pubkey)

    return pubkeys


def search_raw_transactions(address, unconfirmed=True):
    return util.call_jsonrpc_api("search_raw_transactions", {'address': address, 'unconfirmed': unconfirmed}, abort_on_error=True)['result']


def get_unspent_txouts(source, return_confirmed=False):
    """returns a list of unspent outputs for a specific address
    @return: A list of dicts, with each entry in the dict having the following keys:
    """
    txouts = util.call_jsonrpc_api("get_unspent_txouts", {'address': source, 'unconfirmed': True}, abort_on_error=True)['result']
    if return_confirmed:
        return txouts, [output for output in txouts if output['confirmations'] > 0]
    else:
        return txouts


def broadcast_tx(signed_tx_hex):
    return bitcoind_rpc('sendrawtransaction', [signed_tx_hex])
