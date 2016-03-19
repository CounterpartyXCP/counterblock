#! /usr/bin/env python3
"""
server for creating unsigned armory offline transactions
"""
import sys
import logging
import argparse
import json
import time
import threading
import requests
import datetime

import flask
from flask import request
import jsonrpc
from jsonrpc import dispatcher

sys.path.append("/usr/lib/armory/")
from armoryengine.ALL import *

ARMORY_UTXSVR_PORT_MAINNET = 6590
ARMORY_UTXSVR_PORT_TESTNET = 6591
app = flask.Flask(__name__)
is_testnet = False
bitcoind_url = None

def call_rpc(method, params):
    headers = {'content-type': 'application/json'}
    if not isinstance(params, list): params = [params,]
    payload = json.dumps({"method": method, "params": params, "jsonrpc": "2.0", "id": 0})
    response = requests.post(bitcoind_url, headers=headers, data=payload, timeout=10)
    response_json = response.json()
    if 'error' not in response_json.keys() or response_json['error'] == None:
        return response_json['result']
    raise Exception("API request got error response: %s" % response_json)

@dispatcher.add_method
def serialize_unsigned_tx(unsigned_tx_hex, public_key_hex):
    print("REQUEST(serialize_unsigned_tx) -- unsigned_tx_hex: '%s', public_key_hex: '%s'" % (
        unsigned_tx_hex, public_key_hex))

    try:
        unsigned_tx_bin = hex_to_binary(unsigned_tx_hex)
        pytx = PyTx().unserialize(unsigned_tx_bin)

        #compose a txmap manually via bitcoind's getrawtransaction call because armory's way of
        # doing it (TheBDM.bdv().getTxByHash()) seems to not always work in 0.93.3+ ...
        tx_map = {}
        for txin in pytx.inputs:
            outpt = txin.outpoint
            txhash = outpt.txHash
            txhash_hex = binary_to_hex(txhash, BIGENDIAN)
            try:
                raw_tx_result = call_rpc("getrawtransaction", [txhash_hex, 1])
            except Exception as e:
                raise Exception("Could not locate input txhash %s: %s" % (txhash_hex, e))
                return
            tx_map[txhash] = PyTx().unserialize(hex_to_binary(raw_tx_result['hex']))

        utx = UnsignedTransaction(pytx=pytx, pubKeyMap=hex_to_binary(public_key_hex), txMap=tx_map)
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

def blockchainLoaded(args):
    print("**** Initializing Flask (HTTP) server ...")
    app.run(host="127.0.0.1", port=ARMORY_UTXSVR_PORT_MAINNET if not is_testnet else ARMORY_UTXSVR_PORT_TESTNET, threaded=True)
    print("**** Ready to serve ...")

def newBlock(args):
    print('**** NEW BLOCK: Current height is %s' % TheBDM.getTopBlockHeight())

def main():
    global is_testnet, bitcoind_url
    
    print("**** Starting up ...")
    parser = argparse.ArgumentParser(description='Armory offline transaction generator daemon')
    parser.add_argument('--testnet', action='store_true', help='Run for testnet')
    parser.add_argument('bitcoind_url', help='bitcoind RPC endpoint URL, e.g. "http://rpc:rpcpass@localhost:8332"')
    parser_args = parser.parse_args()
    
    btcdir = "/home/xcp/.bitcoin" + ("/testnet3" if parser_args.testnet else '')
    is_testnet = parser_args.testnet
    bitcoind_url = parser_args.bitcoind_url

    print("**** Initializing armory ...")
    #require armory to be installed, adding the configured armory path to PYTHONPATH
    TheBDM.btcdir = btcdir
    TheBDM.RegisterEventForSignal(blockchainLoaded, FINISH_LOAD_BLOCKCHAIN_ACTION)
    TheBDM.RegisterEventForSignal(newBlock, NEW_BLOCK_ACTION)
    TheBDM.goOnline()

    try:
        while(True):
            time.sleep(1)
    except KeyboardInterrupt:
        print("******** Exiting *********")
        exit(0)

if __name__ == '__main__':
    main()
