#! /usr/bin/env python3
"""
server for creating unsigned armory offline transactions
"""
import sys
import logging
import argparse

import flask
from flask import request
sys.path.append("/usr/lib/armory/")
from armoryengine.ALL import *

ARMORY_UTXSVR_PORT_MAINNET = 6590
ARMORY_UTXSVR_PORT_TESTNET = 6591
app = flask.Flask(__name__)

@app.route('/serialize_unsigned_tx/', methods=['GET',])
def serialize_unsigned_tx():
    unsigned_tx = request.args.get('unsigned_tx_hex', None)
    pubkey = request.args.get('public_key_hex', None)
    print("REQUEST(serialize_unsigned_tx) -- unsigned_tx: '%s', pubkey: '%s'" % (unsigned_tx, pubkey))
    if not unsigned_tx or not pubkey:
        return flask.Response("Missing required parameters", 400)

    try:
        unsigned_tx_bin = hex_to_binary(unsigned_tx)
        pytx = PyTx().unserialize(unsigned_tx_bin)
        utx = UnsignedTransaction(pytx=pytx, pubKeyMap=hex_to_binary(pubkey))
        unsigned_tx_ascii = utx.serializeAscii()
    except Exception, e:
        return flask.Response("Could not serialize transaction: %s" % e, 400)
    
    return unsigned_tx_ascii

@app.route('/convert_signed_tx_to_raw_hex/', methods=['GET',])
def convert_signed_tx_to_raw_hex():
    """Converts a signed tx from armory's offline format to a raw hex tx that bitcoind can broadcast/use"""
    signed_tx_ascii = request.args.get('signed_tx_ascii', None)
    print("REQUEST(convert_signed_tx_to_raw_hex) -- signed_tx_ascii: '%s'" % (signed_tx_ascii,))
    if not signed_tx_ascii:
        return flask.Response("Missing required parameters", 400)

    try:
        utx = UnsignedTransaction.unserializeAscii(signed_tx_ascii)
    except Exception, e:
        return flask.Response("Could not decode transaction: %s" % e, 400)
    
    #see if the tx is signed
    if not utx.evaluateSigningStatus().canBroadcast:
        return flask.Response("Passed transaction is not signed", 400)
    
    try:
        raw_tx_bin = UnsignedTransaction.serialize()
        raw_tx_hex = binary_to_hex(raw_tx_bin)
    except Exception, e:
        return flask.Response("Could not serialize transaction: %s" % e, 400)
    
    return raw_tx_hex

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Armory offline transaction generator daemon')
    parser.add_argument('--testnet', action='store_true', help='Run for testnet')
    args = parser.parse_args()
    btcdir = "/home/xcp/.bitcoin%s/" % ('-testnet' if args.testnet else '')

    print("**** Initializing armory...")
    #require armory to be installed, adding the configured armory path to PYTHONPATH
    if not TheBDM.isInitialized():
        TheBDM.btcdir = btcdir
        TheBDM.setBlocking(True)
        TheBDM.setOnlineMode(True)

    print("**** Initializing Flask (HTTP) server...")
    app.run(host="127.0.0.1", port=ARMORY_UTXSVR_PORT_MAINNET if not args.testnet else ARMORY_UTXSVR_PORT_TESTNET)
    print("**** Ready to serve...")
