'''
bitcoind fork of jmcorgan, branch addrindex-0.9.2:
https://github.com/jmcorgan/bitcoin/tree/addrindex-0.9.2
'''
import logging
import binascii
import hashlib
import json

from lib import config, util, util_bitcoin

def is_multisig(address):
    array = address.split('_')
    return (len(array) > 1)

def search_raw_transactions(address):
    return util.call_jsonrpc_api('search_raw_transactions', {'address': address})['result']

def get_unspent_txouts(address, return_confirmed=False):
    return util.call_jsonrpc_api('get_unspent_txouts', {'address': address, 'return_confirmed': return_confirmed})['result']

def get_block_count():
    return int(util.bitcoind_rpc('getblockcount', None))

def check():
    pass

def getinfo():
    return {
        "info": {
            "blocks": get_block_count()
        }
    }

def listunspent(address):
    outputs = get_unspent_txouts(address)
    utxo = []
    for txo in outputs:
        newtxo = {
            'address': address,
            'txid': txo['txid'],
            'vout': txo['vout'],
            'ts': 0,
            'scriptPubKey': txo['scriptPubKey'],
            'amount': float(txo['amount']),
            'confirmations': txo['confirmations'],
            'confirmationsFromCache': False
        }
        utxo.append(newtxo)
    return utxo

def getaddressinfo(address):

    outputs = get_unspent_txouts(address, return_confirmed=True)

    balance = sum(out['amount'] for out in outputs['confirmed'])
    unconfirmed_balance = sum(out['amount'] for out in outputs['all']) - balance
    
    if is_multisig(address):
        array = address.split('_')
        # TODO: filter transactions
        raw_transactions = reversed(search_raw_transactions(array[1:-1][1]))
    else:
        raw_transactions = reversed(search_raw_transactions(address))

    transactions = []
    for tx in raw_transactions:
        if 'confirmations' in tx and tx['confirmations'] > 0:
            transactions.append(tx['txid'])

    return {
        'addrStr': address,
        'balance': balance,
        'balanceSat': balance * config.UNIT,
        'unconfirmedBalance': unconfirmed_balance,
        'unconfirmedBalanceSat': unconfirmed_balance * config.UNIT,
        'transactions': transactions
    }
    
    return None

def gettransaction(tx_hash):
    tx = util.bitcoind_rpc('getrawtransaction', [tx_hash, 1])
    valueOut = 0
    for vout in tx['vout']:
        valueOut += vout['value']
    return {
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

    return None

def get_pubkey_for_address(address):
    #first, get a list of transactions for the address
    address_info = getaddressinfo(address)

    #if no transactions, we can't get the pubkey
    if not address_info['transactions']:
        return None
    
    #for each transaction we got back, extract the vin, pubkey, go through, convert it to binary, and see if it reduces down to the given address
    for tx_id in address_info['transactions']:
        #parse the pubkey out of the first sent transaction
        tx = gettransaction(tx_id)
        for vout in tx['vout']:
            scriptpubkey = vout['scriptPubKey'] 
            pubkey_hex = vout['scriptPubKey']['asm'].split(' ')[1]
            if util_bitcoin.pubkey_to_address(pubkey_hex) == address:
                return pubkey_hex
    return None
