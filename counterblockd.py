#! /usr/bin/env python3
"""
counterblockd server
"""

#import before importing other modules
import gevent
from gevent import monkey; monkey.patch_all()

import sys
import os
import argparse
import json
import logging
import datetime
import time

from lib import config, log, blockfeed, util, module
from lib.processor import StartUpProcessor, messages, caughtup, startup

if __name__ == '__main__':
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(prog='counterblockd', description='Counterwallet daemon. Works with counterpartyd')
    subparsers = parser.add_subparsers(dest='action', help='the action to be taken')
    parser_server = subparsers.add_parser('server', help='Run Counterblockd')
    
    parser_dismod = subparsers.add_parser('dismod', help='Disable a module')
    parser_dismod.add_argument('module_path', type=str, help='Path of module to Disable relative to Counterblockd directory')
    parser_enmod = subparsers.add_parser('enmod', help='Enable a module')
    parser_enmod.add_argument('module_path', type=str, help='Full Path of module to Enable relative to Counterblockd directory')
    parser_listmod = subparsers.add_parser('listmod', help='Display Module Config')
    parser.add_argument('-V', '--version', action='version', version="counterblockd v%s" % config.VERSION)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False, help='sets log level to DEBUG instead of WARNING')
    parser.add_argument('--enmod', type=str, help='Enable a module')
    parser.add_argument('--reparse', action='store_true', default=False, help='force full re-initialization of the counterblockd database')
    parser.add_argument('--testnet', action='store_true', default=False, help='use Bitcoin testnet addresses and block numbers')
    parser.add_argument('--data-dir', help='specify to explicitly override the directory in which to keep the config file and log file')
    parser.add_argument('--config-file', help='the location of the configuration file')
    parser.add_argument('--log-file', help='the location of the log file')
    parser.add_argument('--tx-log-file', help='the location of the transaction log file')
    parser.add_argument('--pid-file', help='the location of the pid file')

    #THINGS WE CONNECT TO
    parser.add_argument('--backend-rpc-connect', help='the hostname or IP of the backend bitcoind JSON-RPC server')
    parser.add_argument('--backend-rpc-port', type=int, help='the backend JSON-RPC port to connect to')
    parser.add_argument('--backend-rpc-user', help='the username used to communicate with backend over JSON-RPC')
    parser.add_argument('--backend-rpc-password', help='the password used to communicate with backend over JSON-RPC')

    parser.add_argument('--counterpartyd-rpc-connect', help='the hostname of the counterpartyd JSON-RPC server')
    parser.add_argument('--counterpartyd-rpc-port', type=int, help='the port used to communicate with counterpartyd over JSON-RPC')
    parser.add_argument('--counterpartyd-rpc-user', help='the username used to communicate with counterpartyd over JSON-RPC')
    parser.add_argument('--counterpartyd-rpc-password', help='the password used to communicate with counterpartyd over JSON-RPC')

    parser.add_argument('--mongodb-connect', help='the hostname of the mongodb server to connect to')
    parser.add_argument('--mongodb-port', type=int, help='the port used to communicate with mongodb')
    parser.add_argument('--mongodb-database', help='the mongodb database to connect to')
    parser.add_argument('--mongodb-user', help='the optional username used to communicate with mongodb')
    parser.add_argument('--mongodb-password', help='the optional password used to communicate with mongodb')

    parser.add_argument('--redis-enable-apicache', action='store_true', default=False, help='set to true to enable caching of API requests')
    parser.add_argument('--redis-connect', help='the hostname of the redis server to use for caching (if enabled')
    parser.add_argument('--redis-port', type=int, help='the port used to connect to the redis server for caching (if enabled)')
    parser.add_argument('--redis-database', type=int, help='the redis database ID (int) used to connect to the redis server for caching (if enabled)')

    parser.add_argument('--armory-utxsvr-enable', help='enable use of armory_utxsvr service (for signing offline armory txns')

    #Vending machine provider
    parser.add_argument('--vending-machine-provider', help='JSON url containing vending machines list')

    #THINGS WE HOST
    parser.add_argument('--rpc-host', help='the IP of the interface to bind to for providing JSON-RPC API access (0.0.0.0 for all interfaces)')
    parser.add_argument('--rpc-port', type=int, help='port on which to provide the counterblockd JSON-RPC API')
    parser.add_argument('--rpc-allow-cors', action='store_true', default=True, help='Allow ajax cross domain request')
    parser.add_argument('--socketio-host', help='the interface on which to host the counterblockd socket.io API')
    parser.add_argument('--socketio-port', type=int, help='port on which to provide the counterblockd socket.io API')
    parser.add_argument('--socketio-chat-host', help='the interface on which to host the counterblockd socket.io chat API')
    parser.add_argument('--socketio-chat-port', type=int, help='port on which to provide the counterblockd socket.io chat API')

    if len(sys.argv) < 2: sys.argv.append('server')
    if not [i for i in sys.argv if i in ('server', 'enmod', 'dismod', 'listmod')]: sys.argv.append('server')

    parser.add_argument('--support-email', help='the email address where support requests should go')
    parser.add_argument('--email-server', help='the email server to send support requests out from. Defaults to \'localhost\'')
    args = parser.parse_args()
    
    config.init(args)

    #Do Module Args Actions
    if args.action == 'enmod':
        module.toggle(args.module_path, True)
        sys.exit(0)
    if args.action == 'dismod': 
        module.toggle(args.module_path, False)
        sys.exit(0)
    if args.action == 'listmod':
        module.list_all()
        sys.exit(0)
        
    #Create/update pid file
    pid = str(os.getpid())
    pidf = open(config.PID, 'w')
    pidf.write(pid)
    pidf.close()    

    log.set_up(args.verbose)
    logging.info("counterblock Version %s starting ..." % config.VERSION)
    
    #Run Startup Functions
    StartUpProcessor.run_active_functions()
