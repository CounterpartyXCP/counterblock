import os
import re
import logging
import binascii
import hashlib
import json
import datetime
import decimal

from repoze.lru import lru_cache
import bitcoin as bitcoinlib
import bitcoin.rpc as bitcoin_rpc
from pycoin import encoding

from lib import config, util

D = decimal.Decimal
decimal.getcontext().prec = 8

def round_out(num):
    """round out to 8 decimal places"""
    return float(D(num))        

def normalize_quantity(quantity, divisible=True):
    """Goes from satoshis to normal human readable format"""
    if divisible:
        return float((D(quantity) / D(config.UNIT))) 
    else: return quantity

def denormalize_quantity(quantity, divisible=True):
    """Goes from normal human readable format to satoshis"""
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

def pubkey_to_address(pubkey_hex):
    sec = binascii.unhexlify(pubkey_hex)
    compressed = encoding.is_sec_compressed(sec)
    public_pair = encoding.sec_to_public_pair(sec)
    address_prefix = b'\x6f' if config.TESTNET else b'\x00'
    return encoding.public_pair_to_bitcoin_address(public_pair, compressed=compressed, address_prefix=address_prefix)

def bitcoind_rpc(command, params):
    return util.call_jsonrpc_api(command, 
                            params = params,
                            endpoint = config.BACKEND_RPC, 
                            auth = config.BACKEND_AUTH, 
                            abort_on_error = True)['result']
                            
def is_multisig(address):
    array = address.split('_')
    return (len(array) > 1)

def get_btc_balance(address, confirmed=True):
    all_unspent, confirmed_unspent = get_unspent_txouts(address, return_confirmed=confirmed)
    unspent = confirmed_unspent if confirmed else all_unspent
    return sum(out['amount'] for out in unspent)

def get_block_count():
    return int(bitcoind_rpc('getblockcount', None))

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
    all_unspent, confirmed_unspent = get_unspent_txouts(address, return_confirmed=True)
    balance = sum(out['amount'] for out in confirmed_unspent)
    unconfirmed_balance = sum(out['amount'] for out in all_unspent) - balance
    
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
    tx = get_cached_raw_transaction(tx_hash)
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

def get_pubkey_from_transactions(address, raw_transactions):
    #for each transaction we got back, extract the vin, pubkey, go through, convert it to binary, and see if it reduces down to the given address
    for tx in raw_transactions:
        #parse the pubkey out of the first sent transaction
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
        if pubkey: pubkeys.append(pubkey)

    return pubkeys


### Unconfirmed Transactions ###

# cache
UNCONFIRMED_ADDRINDEX = {}
OLD_MEMPOOL = []

@lru_cache(maxsize=4096)
def get_cached_raw_transaction(tx_hash):
    return util.bitcoind_rpc('getrawtransaction', [tx_hash, 1])

@lru_cache(maxsize=8192)
def get_cached_batch_raw_transactions(tx_hashes):
    tx_hashes = json.loads(tx_hashes) # for lru_cache
    call_id = 0
    call_list = []
    for tx_hash in tx_hashes:
        call_list.append({
            "method": 'getrawtransaction',
            "params": [tx_hash, 1],
            "jsonrpc": "2.0",
            "id": call_id
        })
        call_id += 1

    if config.TESTNET:
        bitcoinlib.SelectParams('testnet')
    proxy = bitcoin_rpc.Proxy(service_url=config.BACKEND_RPC_URL)
    return proxy._batch(call_list)

# TODO: use scriptpubkey_to_address()
@lru_cache(maxsize=4096)
def extract_addresses(tx):
    tx = json.loads(tx) # for lru_cache
    addresses = []

    for vout in tx['vout']:
        if 'addresses' in vout['scriptPubKey']:
            addresses += vout['scriptPubKey']['addresses']

    vin_hashes = [vin['txid'] for vin in tx['vin']]        
    batch_responses = get_cached_batch_raw_transactions(json.dumps(vin_hashes))
    tx_dict = {}
    for response in batch_responses:
        if 'error' not in response or response['error'] is None:
            if 'result' in response and response['result'] is not None:
                vin_tx = response['result']
                tx_dict[vin_tx['txid']] = vin_tx

    for vin in tx['vin']:
        vin_tx = tx_dict[vin['txid']]
        vout = vin_tx['vout'][vin['vout']]
        if 'addresses' in vout['scriptPubKey']:
            addresses += vout['scriptPubKey']['addresses']

    return addresses

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, D):
            return format(obj, '.8f')
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

def add_tx_to_addrindex(tx):
    global UNCONFIRMED_ADDRINDEX

    addresses = extract_addresses(json.dumps(tx, cls=DecimalEncoder))
    for address in addresses:
        if address not in UNCONFIRMED_ADDRINDEX:
            UNCONFIRMED_ADDRINDEX[address] = {}
        UNCONFIRMED_ADDRINDEX[address][tx['txid']] = tx

def remove_tx_from_addrindex(tx_hash):
    global UNCONFIRMED_ADDRINDEX

    for address in list(UNCONFIRMED_ADDRINDEX.keys()):
        if tx_hash in UNCONFIRMED_ADDRINDEX[address]:
            UNCONFIRMED_ADDRINDEX[address].pop(tx_hash)
            if len(UNCONFIRMED_ADDRINDEX[address]) == 0:
                UNCONFIRMED_ADDRINDEX.pop(address)

def unconfirmed_transactions(address):
    global UNCONFIRMED_ADDRINDEX

    if address in UNCONFIRMED_ADDRINDEX:
        return list(UNCONFIRMED_ADDRINDEX[address].values())
    else:
        return []

def update_unconfirmed_addrindex():
    global OLD_MEMPOOL

    new_mempool = util.bitcoind_rpc('getrawmempool', [])

    # remove confirmed txs
    for tx_hash in OLD_MEMPOOL:
        if tx_hash not in new_mempool:
            remove_tx_from_addrindex(tx_hash)

    # add new txs
    new_tx_hashes = []
    for tx_hash in new_mempool:
        if tx_hash not in OLD_MEMPOOL:
            new_tx_hashes.append(tx_hash)

    if len(new_tx_hashes) > 0:
        batch_responses = get_cached_batch_raw_transactions(json.dumps(new_tx_hashes))
        for response in batch_responses:
            if 'error' not in response or response['error'] is None:
                if 'result' in response and response['result'] is not None:
                    tx = response['result']
                    add_tx_to_addrindex(tx)

    OLD_MEMPOOL = list(new_mempool)

def search_raw_transactions(address):
    unconfirmed = unconfirmed_transactions(address)
    try:
        rawtransactions = util.bitcoind_rpc('searchrawtransactions', [address, 1, 0, 9999999])
    except Exception, e:
        if str(e).find('404') != -1:
            raise Exception('Unknown RPC command: searchrawtransactions. Switch to jmcorgan.')
        else:
            raise Exception(str(e))
    confirmed = [tx for tx in rawtransactions if tx['confirmations'] > 0]
    return unconfirmed + confirmed

### Multi-signature Addresses ###
# NOTE: a pub is either a pubkey or a pubkeyhash

class MultiSigAddressError (Exception):
    pass

def is_multisig(address):
    array = address.split('_')
    return (len(array) > 1)

def canonical_address(address):
    if is_multisig(address):
        signatures_required, pubkeyhashes, signatures_possible = extract_array(address)
        return construct_array(signatures_required, pubkeyhashes, signatures_possible)
    else:
        return address

def test_array(signatures_required, pubs, signatures_possible):
    try:
        signatures_required, signatures_possible = int(signatures_required), int(signatures_possible)
    except ValueError:
        raise MultiSigAddressError('Signature values not integers.')
    if signatures_required < 1 or signatures_required > 3:
        raise MultiSigAddressError('Invalid signatures_required.')
    if signatures_possible < 2 or signatures_possible > 3:
        raise MultiSigAddressError('Invalid signatures_possible.')
    if signatures_possible != len(pubs):
        raise MultiSigAddressError('Incorrect number of pubkeys/pubkeyhashes in multi-signature address.')

def construct_array(signatures_required, pubs, signatures_possible):
    test_array(signatures_required, pubs, signatures_possible)
    address = '_'.join([str(signatures_required)] + sorted(pubs) + [str(signatures_possible)])
    return address

def extract_array(address):
    assert is_multisig(address)
    array = address.split('_')
    signatures_required, pubs, signatures_possible = array[0], sorted(array[1:-1]), array[-1]
    test_array(signatures_required, pubs, signatures_possible)
    return int(signatures_required), pubs, int(signatures_possible)

def pubkeyhash_array(address):
    signatures_required, pubkeyhashes, signatures_possible = extract_array(address)
    return pubkeyhashes

### Multi-signature Addresses ###

def scriptpubkey_to_canonical_address(scriptpubkey):
    if scriptpubkey['type'] == 'multisig':
        asm = scriptpubkey['asm'].split(' ')
        signatures_required = asm[0]
        signatures_possible = len(asm) - 3
        return "_".join([str(signatures_required)] + sorted(scriptpubkey['addresses']) + [str(signatures_possible)])
    else:
        return scriptpubkey['addresses'][0]


def get_unspent_txouts(source, return_confirmed=False):
    """returns a list of unspent outputs for a specific address
    @return: A list of dicts, with each entry in the dict having the following keys:
    """
    # Get all coins.
    outputs = {}
    if is_multisig(source):
        pubkeyhashes = pubkeyhash_array(source)
        raw_transactions = search_raw_transactions(pubkeyhashes[1])
    else:
        pubkeyhashes = [source]
        raw_transactions = search_raw_transactions(source)

    canonical_source = canonical_address(source)

    for tx in raw_transactions:
        for vout in tx['vout']:
            scriptpubkey = vout['scriptPubKey']
            if scriptpubkey_to_canonical_address(scriptpubkey) == canonical_source:
                txid = tx['txid']
                confirmations = tx['confirmations'] if 'confirmations' in tx else 0
                outkey = '{}{}'.format(txid, vout['n'])
                if outkey not in outputs or outputs[outkey]['confirmations'] < confirmations:
                    coin = {'amount': float(vout['value']),
                            'confirmations': confirmations,
                            'scriptPubKey': scriptpubkey['hex'],
                            'txid': txid,
                            'vout': vout['n']
                           }
                    outputs[outkey] = coin
    outputs = outputs.values()

    # Prune away spent coins.
    unspent = []
    confirmed_unspent = []
    for output in outputs:
        spent = False
        confirmed_spent = False
        for tx in raw_transactions:
            for vin in tx['vin']:
                if 'coinbase' in vin: continue
                if (vin['txid'], vin['vout']) == (output['txid'], output['vout']):
                    spent = True
                    if 'confirmations' in tx and tx['confirmations'] > 0:
                        confirmed_spent = True
        if not spent:
            unspent.append(output)
        if not confirmed_spent and output['confirmations'] > 0:
            confirmed_unspent.append(output)

    unspent = sorted(unspent, key=lambda x: x['txid'])
    confirmed_unspent = sorted(confirmed_unspent, key=lambda x: x['txid'])

    if return_confirmed:
        return unspent, confirmed_unspent
    else:
        return unspent


