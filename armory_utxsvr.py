#! /usr/bin/env python3
"""
server for creating unsigned armory offline transactions
"""
import sys
import logging
import argparse
import json

import flask
from flask import request
import jsonrpc
from jsonrpc import dispatcher

sys.path.append("/usr/lib/armory/")
from armoryengine.ALL import *

ARMORY_UTXSVR_PORT_MAINNET = 6590
ARMORY_UTXSVR_PORT_TESTNET = 6591
app = flask.Flask(__name__)

@dispatcher.add_method
def serialize_unsigned_tx(unsigned_tx_hex, public_key_hex):
    print("REQUEST(serialize_unsigned_tx) -- unsigned_tx_hex: '%s', public_key_hex: '%s'" % (
        unsigned_tx_hex, public_key_hex))

    try:
        unsigned_tx_bin = hex_to_binary(unsigned_tx_hex)
        pytx = PyTx().unserialize(unsigned_tx_bin)
        utx = UnsignedTransaction(pytx=pytx, pubKeyMap=hex_to_binary(public_key_hex))
        unsigned_tx_ascii = utx.serializeAscii()
    except Exception, e:
        raise Exception("Could not serialize transaction: %s" % e)
    
    return unsigned_tx_ascii

@dispatcher.add_method
def convert_signed_tx_to_raw_hex(signed_tx_ascii):
    """Converts a signed tx from armory's offline format to a raw hex tx that bitcoind can broadcast/use"""
    print("REQUEST(convert_signed_tx_to_raw_hex) -- signed_tx_ascii:\n'%s'\n" % (signed_tx_ascii,))

    try:
        utx = UnsignedTransaction()
        utx.unserializeAscii(signed_tx_ascii)
    except Exception, e:
        raise Exception("Could not decode transaction: %s" % e)
    
    #see if the tx is signed
    if not utx.evaluateSigningStatus().canBroadcast:
        raise Exception("Passed transaction is not signed")
    
    try:
        pytx = utx.getSignedPyTx()
        raw_tx_bin = pytx.serialize()
        raw_tx_hex = binary_to_hex(raw_tx_bin)
    except Exception, e:
        raise Exception("Could not serialize transaction: %s" % e)
    
    return raw_tx_hex

@app.route('/', methods=["POST",])
@app.route('/api/', methods=["POST",])
def handle_post():
    request_json = flask.request.get_data().decode('utf-8')
    rpc_response = jsonrpc.JSONRPCResponseManager.handle(request_json, dispatcher)
    rpc_response_json = json.dumps(rpc_response.data).encode()
    response = flask.Response(rpc_response_json, 200, mimetype='application/json')
    return response
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Armory offline transaction generator daemon')
    parser.add_argument('--testnet', action='store_true', help='Run for testnet')
    args = parser.parse_args()
    btcdir = "/home/xcp/.bitcoin%s/" % ('-testnet/testnet3' if args.testnet else '')

    print("**** Initializing armory...")
    #require armory to be installed, adding the configured armory path to PYTHONPATH
    TheBDM.btcdir = btcdir
    TheBDM.setBlocking(True)
    TheBDM.setOnlineMode(True)

    print("**** Initializing Flask (HTTP) server...")
    app.run(host="127.0.0.1", port=ARMORY_UTXSVR_PORT_MAINNET if not args.testnet else ARMORY_UTXSVR_PORT_TESTNET)
    print("**** Ready to serve...")
