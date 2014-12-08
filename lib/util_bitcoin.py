import os
import re
import json
import logging
import datetime
import decimal
import binascii

from pycoin import encoding

from lib import config

D = decimal.Decimal
decimal.getcontext().prec = 8

def round_out(num):
    #round out to 8 decimal places
    return float(D(num))        

def normalize_quantity(quantity, divisible=True):
    if divisible:
        return float((D(quantity) / D(config.UNIT))) 
    else: return quantity

def denormalize_quantity(quantity, divisible=True):
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
