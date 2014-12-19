'''
bitcoind fork of jmcorgan, branch addrindex-0.9.2:
https://github.com/jmcorgan/bitcoin/tree/addrindex-0.9.2
'''
import logging
import binascii
import hashlib
import json
from repoze.lru import lru_cache

from lib import config, util, util_bitcoin

def is_multisig(address):
    array = address.split('_')
    return (len(array) > 1)

def get_btc_balance(address, confirmed=True):
    all_unspent, confirmed_unspent = get_unspent_txouts(address, return_confirmed=confirmed)
    unspent = confirmed_unspent if confirmed else all_unspent
    return sum(out['amount'] for out in unspent)

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
                if util_bitcoin.pubkey_to_address(pubkey_hex) == address:
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

# TODO: use scriptpubkey_to_address()
@lru_cache(maxsize=4096)
def extract_addresses(tx):
    tx = json.loads(tx) # for lru_cache
    addresses = []

    for vout in tx['vout']:
        if 'addresses' in vout['scriptPubKey']:
            addresses += vout['scriptPubKey']['addresses']

    for vin in tx['vin']:
        vin_tx = get_cached_raw_transaction(vin['txid'])
        vout = vin_tx['vout'][vin['vout']]
        if 'addresses' in vout['scriptPubKey']:
            addresses += vout['scriptPubKey']['addresses']

    return addresses

def add_tx_to_addrindex(tx):
    global UNCONFIRMED_ADDRINDEX

    addresses = extract_addresses(json.dumps(tx))
    for address in addresses:
        if address not in UNCONFIRMED_ADDRINDEX:
            UNCONFIRMED_ADDRINDEX[address] = {}
        UNCONFIRMED_ADDRINDEX[address][tx['txid']] = tx

def remove_tx_from_addrindex(tx):
    global UNCONFIRMED_ADDRINDEX

    for address in list(UNCONFIRMED_ADDRINDEX.keys()):
        if tx['txid'] in UNCONFIRMED_ADDRINDEX[address]:
            UNCONFIRMED_ADDRINDEX[address].pop(tx['txid'])
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
            tx = get_cached_raw_transaction(tx_hash)
            remove_tx_from_addrindex(tx)
    # add new txs
    for tx_hash in new_mempool:
        if tx_hash not in OLD_MEMPOOL:
            tx = get_cached_raw_transaction(tx_hash)
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


