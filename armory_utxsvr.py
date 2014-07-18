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

@app.route('/', methods=['GET',])
def serialize_unsigned_tx():
    unsigned_tx = request.args.get('unsigned_tx_hex', None)
    pubkey = request.args.get('public_key_hex', None)
    logging.info("REQUEST -- unsigned_tx: '%s', pubkey: '%s'" % (unsigned_tx, pubkey))
    if not unsigned_tx or not pubkey: flask.abort(400)

    unsigned_tx_bin = hex_to_binary(unsigned_tx)
    pytx = PyTx().unserialize(unsigned_tx_bin)
    utx = UnsignedTransaction(pytx=pytx, pubKeyMap=hex_to_binary(pubkey))
    unsigned_tx_ascii = utx.serializeAscii()
    return(unsigned_tx_ascii)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    parser = argparse.ArgumentParser(description='Armory offline transaction generator daemon')
    parser.add_argument('--testnet', action='store_true', help='Run for testnet')
    args = parser.parse_args()
    btcdir = "/home/xcp/.bitcoin%s/" % ('-testnet' if args.testnet else '')

    logging.info("Initializing armory...")
    #require armory to be installed, adding the configured armory path to PYTHONPATH
    if not TheBDM.isInitialized():
        TheBDM.btcdir = btcdir
        TheBDM.setBlocking(True)
        TheBDM.setOnlineMode(True)

    logging.info("Initializing Flask server...")
    app.run(host="127.0.0.1:%s" % (ARMORY_UTXSVR_PORT_MAINNET if not args.testnet else ARMORY_UTXSVR_PORT_TESTNET))