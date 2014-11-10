'''
bitcoind fork of jmcorgan, branch addrindex-0.9.2:
https://github.com/jmcorgan/bitcoin/tree/addrindex-0.9.2
'''
import logging
import binascii
import hashlib
import json

from lib import config, util, util_bitcoin

b58_digits = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'

dhash = lambda x: hashlib.sha256(hashlib.sha256(x).digest()).digest()

def base58_check_decode (s, version):
    # Convert the string to an integer
    n = 0
    for c in s:
        n *= 58
        if c not in b58_digits:
            raise Exception('Not a valid base58 character:', c)
        digit = b58_digits.index(c)
        n += digit

    # Convert the integer to bytes
    h = '%x' % n
    if len(h) % 2:
        h = '0' + h
    res = binascii.unhexlify(h.encode('utf8'))

    # Add padding back.
    pad = 0
    for c in s[:-1]:
        if c == b58_digits[0]: pad += 1
        else: break
    k = version * pad + res

    addrbyte, data, chk0 = k[0:1], k[1:-4], k[-4:]
    if addrbyte != version:
        raise Exception('incorrect version byte')
    chk1 = dhash(addrbyte + data)[:4]
    if chk0 != chk1:
        raise Exception('Checksum mismatch: %r not equal %r' % (chk0, chk1))
    return data

def is_multisig(address):
    array = address.split('_')
    return (len(array) > 1)

def test_array(signatures_required, pubs, signatures_possible):
    try:
        signatures_required, signatures_possible = int(signatures_required), int(signatures_possible)
    except ValueError:
        raise Exception('Signature values not integers.')
    if signatures_required < 1 or signatures_required > 3:
        raise Exception('Invalid signatures_required.')
    if signatures_possible < 2 or signatures_possible > 3:
        raise Exception('Invalid signatures_possible.')
    if signatures_possible != len(pubs):
        raise Exception('Incorrect number of pubkeys/pubkeyhashes in multi-signature address.')

def extract_array(address):
    assert is_multisig(address)
    array = address.split('_')
    signatures_required, pubs, signatures_possible = array[0], sorted(array[1:-1]), array[-1]
    test_array(signatures_required, pubs, signatures_possible)
    return int(signatures_required), pubs, int(signatures_possible)

def pubkeyhash_array(address):
    signatures_required, pubkeyhashes, signatures_possible = extract_array(address)
    if not all([base58_check_decode(pubkeyhash, b'\x6f' if config.TESTNET else b'\x00') for pubkeyhash in pubkeyhashes]):
        raise Exception('Multi-signature address must use PubKeyHashes, not public keys.')
    return pubkeyhashes

def search_raw_transactions(address):
    return util.bitcoind_rpc('searchrawtransactions', [address, 1, 0, 9999999])

def get_unspent_txouts(source):
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

    for tx in raw_transactions:
        for vout in tx['vout']:
            scriptpubkey = vout['scriptPubKey']
            if is_multisig(source) and scriptpubkey['type'] != 'multisig':
                continue
            elif 'addresses' in scriptpubkey.keys() and "".join(sorted(scriptpubkey['addresses'])) == "".join(sorted(pubkeyhashes)):
                txid = tx['txid']
                if txid not in outputs or outputs[txid]['confirmations'] < tx['confirmations']:
                    coin = {'amount': vout['value'],
                            'confirmations': tx['confirmations'],
                            'scriptPubKey': scriptpubkey['hex'],
                            'txid': txid,
                            'vout': vout['n']
                           }
                    outputs[txid] = coin
    outputs = outputs.values()

    # Prune away spent coins.
    unspent = []
    for output in outputs:
        spent = False
        for tx in raw_transactions:
            for vin in tx['vin']:
                if (vin['txid'], vin['vout']) == (output['txid'], output['vout']):
                    spent = True
        if not spent:
            unspent.append(output)

    return unspent

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

    outputs = get_unspent_txouts(address)
    balance = sum(out['amount'] for out in outputs)
    raw_transactions = reversed(search_raw_transactions(address))
    transactions = [tx['txid'] for tx in raw_transactions]

    return {
        'addrStr': address,
        'balance': balance,
        'balanceSat': balance * config.UNIT,
        'unconfirmedBalance': 0,
        'unconfirmedBalanceSat': 0,
        'unconfirmedTxApperances': 0,
        'txApperances': len(transactions),
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
        'confirmations': tx['confirmations'],
        'blocktime': tx['blocktime'],
        'blockhash': tx['blockhash'],
        'time': tx['time'],
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
