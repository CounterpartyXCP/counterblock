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
import ConfigParser
import time
from configobj import ConfigObj
import imp
import email.utils

import appdirs
import pymongo
import zmq.green as zmq
import rollbar
import redis
import redis.connection
redis.connection.socket = gevent.socket #make redis play well with gevent

from socketio import server as socketio_server
import pygeoip

from lib import (config, api, events, blockfeed, siofeeds, util)
from lib.processor import processor
from lib.processor import messages, caughtup, startup

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

    parser.add_argument('--rollbar-token', help='the API token to use with rollbar (leave blank to disable rollbar integration)')
    parser.add_argument('--rollbar-env', help='the environment name for the rollbar integration (if enabled). Defaults to \'production\'')

    if len(sys.argv) < 2: sys.argv.append('server')
    if not [i for i in sys.argv if i in ('server', 'enmod', 'dismod', 'listmod')]: sys.argv.append('server')

    parser.add_argument('--support-email', help='the email address where support requests should go')
    parser.add_argument('--email-server', help='the email server to send support requests out from. Defaults to \'localhost\'')
    args = parser.parse_args()

    # Data directory
    if not args.data_dir:
        config.DATA_DIR = appdirs.user_data_dir(appauthor='Counterparty', appname='counterblockd', roaming=True)
    else:
        config.DATA_DIR = args.data_dir
    if not os.path.isdir(config.DATA_DIR): os.mkdir(config.DATA_DIR)

    #Read config file
    configfile = ConfigParser.ConfigParser()
    config_path = os.path.join(config.DATA_DIR, 'counterblockd.conf')
    configfile.read(config_path)
    has_config = configfile.has_section('Default')
    
    #Do Module Args Actions
    
    def toggle_mod(mod, enabled=True):
        try:
            imp.find_module(mod)
        except: 
            print("Unable to find module %s" %mod)
            return
        mod_config_path = os.path.join(config.DATA_DIR, 'counterblockd_module.conf')
        module_conf = ConfigObj(mod_config_path)
        try:
            try:
                if module_conf['LoadModule'][mod][0] in ['True', 'False']: 
                    module_conf['LoadModule'][mod][0] = enabled
                else: module_conf['LoadModule'][mod][1] = enabled
            except: module_conf['LoadModule'][mod].insert(0, enabled)
        except: 
            if not "LoadModule" in module_conf: module_conf['LoadModule'] = {}
            module_conf['LoadModule'][mod] = enabled 
        module_conf.write()
        print("%s Module %s" %("Enabled" if enabled else "Disabled", mod))
        
    def list_mod():
        mod_config_path = os.path.join(config.DATA_DIR, 'counterblockd_module.conf')
        module_conf = ConfigObj(mod_config_path)
        for name, modules in module_conf.items(): 
            print("Configuration for %s" %name)
            for module, settings in modules.items(): 
                print("     %s %s: %s" %(("Module" if name == "LoadModule" else "Function"), module, settings))
                
    if args.action == 'enmod':
        toggle_mod(args.module_path, True)
        sys.exit(1)
    if args.action == 'dismod': 
        toggle_mod(args.module_path, False)
        sys.exit(1)
    if args.action == 'listmod':
        list_mod()
        sys.exit(1)
        
    # testnet
    if args.testnet:
        config.TESTNET = args.testnet
    elif has_config and configfile.has_option('Default', 'testnet'):
        config.TESTNET = configfile.getboolean('Default', 'testnet')
    else:
        config.TESTNET = False
        
    # reparse
    config.REPARSE_FORCED = args.reparse
        
    ##############
    # THINGS WE CONNECT TO

    # backend (e.g. bitcoind) RPC host
    if args.counterpartyd_rpc_connect:
        config.BACKEND_RPC_CONNECT = args.backend_rpc_connect
    elif has_config and configfile.has_option('Default', 'backend-rpc-connect') and configfile.get('Default', 'backend-rpc-connect'):
        config.BACKEND_RPC_CONNECT = configfile.get('Default', 'backend-rpc-connect')
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-connect') and configfile.get('Default', 'bitcoind-rpc-connect'):
        config.BACKEND_RPC_CONNECT = configfile.get('Default', 'bitcoind-rpc-connect')
    else:
        config.BACKEND_RPC_CONNECT = 'localhost'

    # backend (e.g. bitcoind) RPC port
    if args.backend_rpc_port:
        config.BACKEND_RPC_PORT = args.backend_rpc_port
    elif has_config and configfile.has_option('Default', 'backend-rpc-port') and configfile.get('Default', 'backend-rpc-port'):
        config.BACKEND_RPC_PORT = configfile.get('Default', 'backend-rpc-port')
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-port') and configfile.get('Default', 'bitcoind-rpc-port'):
        config.BACKEND_RPC_PORT = configfile.get('Default', 'bitcoind-rpc-port')
    else:
        if config.TESTNET:
            config.BACKEND_RPC_PORT = config.DEFAULT_BACKEND_RPC_PORT_TESTNET
        else:
            config.BACKEND_RPC_PORT = config.DEFAULT_BACKEND_RPC_PORT
    try:
        config.BACKEND_RPC_PORT = int(config.BACKEND_RPC_PORT)
        assert int(config.BACKEND_RPC_PORT) > 1 and int(config.BACKEND_RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number backend-rpc-port configuration parameter")
            
    # backend (e.g. bitcoind) RPC user
    if args.backend_rpc_user:
        config.BACKEND_RPC_USER = args.backend_rpc_user
    elif has_config and configfile.has_option('Default', 'backend-rpc-user') and configfile.get('Default', 'backend-rpc-user'):
        config.BACKEND_RPC_USER = configfile.get('Default', 'backend-rpc-user')
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-user') and configfile.get('Default', 'bitcoind-rpc-user'):
        config.BACKEND_RPC_USER = configfile.get('Default', 'bitcoind-rpc-user')
    else:
        config.BACKEND_RPC_USER = 'rpcuser'

    # backend (e.g. bitcoind) RPC password
    if args.backend_rpc_password:
        config.BACKEND_RPC_PASSWORD = args.backend_rpc_password
    elif has_config and configfile.has_option('Default', 'backend-rpc-password') and configfile.get('Default', 'backend-rpc-password'):
        config.BACKEND_RPC_PASSWORD = configfile.get('Default', 'backend-rpc-password')
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-password') and configfile.get('Default', 'bitcoind-rpc-password'):
        config.BACKEND_RPC_PASSWORD = configfile.get('Default', 'bitcoind-rpc-password')
    else:
        config.BACKEND_RPC_PASSWORD = 'rpcpassword'

    config.BACKEND_RPC = 'http://' + config.BACKEND_RPC_CONNECT + ':' + str(config.BACKEND_RPC_PORT) + '/'
    config.BACKEND_AUTH = (config.BACKEND_RPC_USER, config.BACKEND_RPC_PASSWORD) if (config.BACKEND_RPC_USER and config.BACKEND_RPC_PASSWORD) else None


    # counterpartyd RPC host
    if args.counterpartyd_rpc_connect:
        config.COUNTERPARTYD_RPC_CONNECT = args.counterpartyd_rpc_connect
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-connect') and configfile.get('Default', 'counterpartyd-rpc-connect'):
        config.COUNTERPARTYD_RPC_CONNECT = configfile.get('Default', 'counterpartyd-rpc-connect')
    else:
        config.COUNTERPARTYD_RPC_CONNECT = 'localhost'

    # counterpartyd RPC port
    if args.counterpartyd_rpc_port:
        config.COUNTERPARTYD_RPC_PORT = args.counterpartyd_rpc_port
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-port') and configfile.get('Default', 'counterpartyd-rpc-port'):
        config.COUNTERPARTYD_RPC_PORT = configfile.get('Default', 'counterpartyd-rpc-port')
    else:
        if config.TESTNET:
            config.COUNTERPARTYD_RPC_PORT = 14000
        else:
            config.COUNTERPARTYD_RPC_PORT = 4000
    try:
        config.COUNTERPARTYD_RPC_PORT = int(config.COUNTERPARTYD_RPC_PORT)
        assert int(config.COUNTERPARTYD_RPC_PORT) > 1 and int(config.COUNTERPARTYD_RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number counterpartyd-rpc-port configuration parameter")
    
    # counterpartyd RPC user
    if args.counterpartyd_rpc_user:
        config.COUNTERPARTYD_RPC_USER = args.counterpartyd_rpc_user
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-user') and configfile.get('Default', 'counterpartyd-rpc-user'):
        config.COUNTERPARTYD_RPC_USER = configfile.get('Default', 'counterpartyd-rpc-user')
    else:
        config.COUNTERPARTYD_RPC_USER = 'rpcuser'

    # counterpartyd RPC password
    if args.counterpartyd_rpc_password:
        config.COUNTERPARTYD_RPC_PASSWORD = args.counterpartyd_rpc_password
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-password') and configfile.get('Default', 'counterpartyd-rpc-password'):
        config.COUNTERPARTYD_RPC_PASSWORD = configfile.get('Default', 'counterpartyd-rpc-password')
    else:
        config.COUNTERPARTYD_RPC_PASSWORD = 'rpcpassword'

    config.COUNTERPARTYD_RPC = 'http://' + config.COUNTERPARTYD_RPC_CONNECT + ':' + str(config.COUNTERPARTYD_RPC_PORT) + '/api/'
    config.COUNTERPARTYD_AUTH = (config.COUNTERPARTYD_RPC_USER, config.COUNTERPARTYD_RPC_PASSWORD) if (config.COUNTERPARTYD_RPC_USER and config.COUNTERPARTYD_RPC_PASSWORD) else None

    # mongodb host
    if args.mongodb_connect:
        config.MONGODB_CONNECT = args.mongodb_connect
    elif has_config and configfile.has_option('Default', 'mongodb-connect') and configfile.get('Default', 'mongodb-connect'):
        config.MONGODB_CONNECT = configfile.get('Default', 'mongodb-connect')
    else:
        config.MONGODB_CONNECT = 'localhost'

    # mongodb port
    if args.mongodb_port:
        config.MONGODB_PORT = args.mongodb_port
    elif has_config and configfile.has_option('Default', 'mongodb-port') and configfile.get('Default', 'mongodb-port'):
        config.MONGODB_PORT = configfile.get('Default', 'mongodb-port')
    else:
        config.MONGODB_PORT = 27017
    try:
        config.MONGODB_PORT = int(config.MONGODB_PORT)
        assert int(config.MONGODB_PORT) > 1 and int(config.MONGODB_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number mongodb-port configuration parameter")
            
    # mongodb database
    if args.mongodb_database:
        config.MONGODB_DATABASE = args.mongodb_database
    elif has_config and configfile.has_option('Default', 'mongodb-database') and configfile.get('Default', 'mongodb-database'):
        config.MONGODB_DATABASE = configfile.get('Default', 'mongodb-database')
    else:
        if config.TESTNET:
            config.MONGODB_DATABASE = 'counterblockd_testnet'
        else:
            config.MONGODB_DATABASE = 'counterblockd'

    # mongodb user
    if args.mongodb_user:
        config.MONGODB_USER = args.mongodb_user
    elif has_config and configfile.has_option('Default', 'mongodb-user') and configfile.get('Default', 'mongodb-user'):
        config.MONGODB_USER = configfile.get('Default', 'mongodb-user')
    else:
        config.MONGODB_USER = None

    # mongodb password
    if args.mongodb_password:
        config.MONGODB_PASSWORD = args.mongodb_password
    elif has_config and configfile.has_option('Default', 'mongodb-password') and configfile.get('Default', 'mongodb-password'):
        config.MONGODB_PASSWORD = configfile.get('Default', 'mongodb-password')
    else:
        config.MONGODB_PASSWORD = None

    # redis-enable-apicache
    if args.redis_enable_apicache:
        config.REDIS_ENABLE_APICACHE = args.redis_enable_apicache
    elif has_config and configfile.has_option('Default', 'redis-enable-apicache') and configfile.get('Default', 'redis-enable-apicache'):
        config.REDIS_ENABLE_APICACHE = configfile.getboolean('Default', 'redis-enable-apicache')
    else:
        config.REDIS_ENABLE_APICACHE = False

    # redis connect
    if args.redis_connect:
        config.REDIS_CONNECT = args.redis_connect
    elif has_config and configfile.has_option('Default', 'redis-connect') and configfile.get('Default', 'redis-connect'):
        config.REDIS_CONNECT = configfile.get('Default', 'redis-connect')
    else:
        config.REDIS_CONNECT = '127.0.0.1'

    # redis port
    if args.redis_port:
        config.REDIS_PORT = args.redis_port
    elif has_config and configfile.has_option('Default', 'redis-port') and configfile.get('Default', 'redis-port'):
        config.REDIS_PORT = configfile.get('Default', 'redis-port')
    else:
        config.REDIS_PORT = 6379
    try:
        config.REDIS_PORT = int(config.REDIS_PORT)
        assert int(config.REDIS_PORT) > 1 and int(config.REDIS_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number redis-port configuration parameter")

    # redis database
    if args.redis_database:
        config.REDIS_DATABASE = args.redis_database
    elif has_config and configfile.has_option('Default', 'redis-database') and configfile.get('Default', 'redis-database'):
        config.REDIS_DATABASE = configfile.get('Default', 'redis-database')
    else:
        if config.TESTNET:
            config.REDIS_DATABASE = 1
        else:
            config.REDIS_DATABASE = 0
    try:
        config.REDIS_DATABASE = int(config.REDIS_DATABASE)
        assert int(config.REDIS_DATABASE) >= 0 and int(config.REDIS_DATABASE) <= 16
    except:
        raise Exception("Please specific a valid redis-database configuration parameter (between 0 and 16 inclusive)")

    # redis connect
    if args.armory_utxsvr_enable:
        config.ARMORY_UTXSVR_ENABLE = args.armory_utxsvr_enable
    elif has_config and configfile.has_option('Default', 'armory-utxsvr-enable') and configfile.getboolean('Default', 'armory-utxsvr-enable'):
        config.ARMORY_UTXSVR_ENABLE = configfile.get('Default', 'armory-utxsvr-enable')
    else:
        config.ARMORY_UTXSVR_ENABLE = False

    if args.vending_machine_provider:
        config.VENDING_MACHINE_PROVIDER = args.vending_machine_provider
    elif has_config and configfile.has_option('Default', 'vending-machine-provider') and configfile.get('Default', 'vending-machine-provider'):
        config.VENDING_MACHINE_PROVIDER = configfile.get('Default', 'vending-machine-provider')
    else:
        config.VENDING_MACHINE_PROVIDER = None

    ##############
    # THINGS WE SERVE
    
    # RPC host
    if args.rpc_host:
        config.RPC_HOST = args.rpc_host
    elif has_config and configfile.has_option('Default', 'rpc-host') and configfile.get('Default', 'rpc-host'):
        config.RPC_HOST = configfile.get('Default', 'rpc-host')
    else:
        config.RPC_HOST = 'localhost'

    # RPC port
    if args.rpc_port:
        config.RPC_PORT = args.rpc_port
    elif has_config and configfile.has_option('Default', 'rpc-port') and configfile.get('Default', 'rpc-port'):
        config.RPC_PORT = configfile.get('Default', 'rpc-port')
    else:
        if config.TESTNET:
            config.RPC_PORT = 14100
        else:
            config.RPC_PORT = 4100        
    try:
        config.RPC_PORT = int(config.RPC_PORT)
        assert int(config.RPC_PORT) > 1 and int(config.RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number rpc-port configuration parameter")

     # RPC CORS
    if args.rpc_allow_cors:
        config.RPC_ALLOW_CORS = args.rpc_allow_cors
    elif has_config and configfile.has_option('Default', 'rpc-allow-cors'):
        config.RPC_ALLOW_CORS = configfile.getboolean('Default', 'rpc-allow-cors')
    else:
        config.RPC_ALLOW_CORS = True

    # socket.io host
    if args.socketio_host:
        config.SOCKETIO_HOST = args.socketio_host
    elif has_config and configfile.has_option('Default', 'socketio-host') and configfile.get('Default', 'socketio-host'):
        config.SOCKETIO_HOST = configfile.get('Default', 'socketio-host')
    else:
        config.SOCKETIO_HOST = 'localhost'

    # socket.io port
    if args.socketio_port:
        config.SOCKETIO_PORT = args.socketio_port
    elif has_config and configfile.has_option('Default', 'socketio-port') and configfile.get('Default', 'socketio-port'):
        config.SOCKETIO_PORT = configfile.get('Default', 'socketio-port')
    else:
        if config.TESTNET:
            config.SOCKETIO_PORT = 14101
        else:
            config.SOCKETIO_PORT = 4101        
    try:
        config.SOCKETIO_PORT = int(config.SOCKETIO_PORT)
        assert int(config.SOCKETIO_PORT) > 1 and int(config.SOCKETIO_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number socketio-port configuration parameter")

    # socket.io chat host
    if args.socketio_chat_host:
        config.SOCKETIO_CHAT_HOST = args.socketio_chat_host
    elif has_config and configfile.has_option('Default', 'socketio-chat-host') and configfile.get('Default', 'socketio-chat-host'):
        config.SOCKETIO_CHAT_HOST = configfile.get('Default', 'socketio-chat-host')
    else:
        config.SOCKETIO_CHAT_HOST = 'localhost'

    # socket.io chat port
    if args.socketio_chat_port:
        config.SOCKETIO_CHAT_PORT = args.socketio_chat_port
    elif has_config and configfile.has_option('Default', 'socketio-chat-port') and configfile.get('Default', 'socketio-chat-port'):
        config.SOCKETIO_CHAT_PORT = configfile.get('Default', 'socketio-chat-port')
    else:
        if config.TESTNET:
            config.SOCKETIO_CHAT_PORT = 14102
        else:
            config.SOCKETIO_CHAT_PORT = 4102       
    try:
        config.SOCKETIO_CHAT_PORT = int(config.SOCKETIO_CHAT_PORT)
        assert int(config.SOCKETIO_CHAT_PORT) > 1 and int(config.SOCKETIO_CHAT_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number socketio-chat-port configuration parameter")


    ##############
    # OTHER SETTINGS

    #More testnet
    if config.TESTNET:
        config.BLOCK_FIRST = 281000
    else:
        config.BLOCK_FIRST = 278270

    # Log
    if args.log_file:
        config.LOG = args.log_file
    elif has_config and configfile.has_option('Default', 'log-file'):
        config.LOG = configfile.get('Default', 'log-file')
    else:
        config.LOG = os.path.join(config.DATA_DIR, 'counterblockd.log')
        
    if args.tx_log_file:
        config.TX_LOG = args.tx_log_file
    elif has_config and configfile.has_option('Default', 'tx-log-file'):
        config.TX_LOG = configfile.get('Default', 'tx-log-file')
    else:
        config.TX_LOG = os.path.join(config.DATA_DIR, 'counterblockd-tx.log')
    

    # PID
    if args.pid_file:
        config.PID = args.pid_file
    elif has_config and configfile.has_option('Default', 'pid-file'):
        config.PID = configfile.get('Default', 'pid-file')
    else:
        config.PID = os.path.join(config.DATA_DIR, 'counterblockd.pid')

     # ROLLBAR INTEGRATION
    if args.rollbar_token:
        config.ROLLBAR_TOKEN = args.rollbar_token
    elif has_config and configfile.has_option('Default', 'rollbar-token'):
        config.ROLLBAR_TOKEN = configfile.get('Default', 'rollbar-token')
    else:
        config.ROLLBAR_TOKEN = None #disable rollbar integration

    if args.rollbar_env:
        config.ROLLBAR_ENV = args.rollbar_env
    elif has_config and configfile.has_option('Default', 'rollbar-env'):
        config.ROLLBAR_ENV = configfile.get('Default', 'rollbar-env')
    else:
        config.ROLLBAR_ENV = 'counterblockd-production'
        
    #support email
    if args.support_email:
        config.SUPPORT_EMAIL = args.support_email
    elif has_config and configfile.has_option('Default', 'support-email') and configfile.get('Default', 'support-email'):
        config.SUPPORT_EMAIL = configfile.get('Default', 'support-email')
    else:
        config.SUPPORT_EMAIL = None #disable support tickets
    if config.SUPPORT_EMAIL:
        if not email.utils.parseaddr(config.SUPPORT_EMAIL)[1]:
            raise Exception("Invalid support email address")

    #email server
    if args.email_server:
        config.EMAIL_SERVER = args.email_server
    elif has_config and configfile.has_option('Default', 'email-server') and configfile.get('Default', 'email-server'):
        config.EMAIL_SERVER = configfile.get('Default', 'email-server')
    else:
        config.EMAIL_SERVER = "localhost"

    # current dir
    config.COUNTERBLOCKD_DIR = os.path.dirname(os.path.realpath(__file__))
    
    # initialize json schema for json asset and feed validation
    config.ASSET_SCHEMA = json.load(open(os.path.join(config.COUNTERBLOCKD_DIR, 'schemas', 'asset.schema.json')))
    config.FEED_SCHEMA = json.load(open(os.path.join(config.COUNTERBLOCKD_DIR, 'schemas', 'feed.schema.json')))

    #Create/update pid file
    pid = str(os.getpid())
    pidf = open(config.PID, 'w')
    pidf.write(pid)
    pidf.close()    

    # Logging (to file and console).
    MAX_LOG_SIZE = 20 * 1024 * 1024 #max log size of 20 MB before rotation (make configurable later)
    MAX_LOG_COUNT = 5
    logger = logging.getLogger() #get root logger
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    
    #Color logging on console for warnings and errors
    logging.addLevelName( logging.WARNING, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
    logging.addLevelName( logging.ERROR, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
    
    #Console logging
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)
    #File logging (rotated)
    if os.name == 'nt':
        fileh = util_windows.SanitizedRotatingFileHandler(config.LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
    else:
        fileh = logging.handlers.RotatingFileHandler(config.LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
    fileh.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(message)s', '%Y-%m-%d-T%H:%M:%S%z')
    fileh.setFormatter(formatter)
    logger.addHandler(fileh)
    #socketio logging (don't show on console in normal operation)
    socketio_log = logging.getLogger('socketio')
    socketio_log.setLevel(logging.DEBUG if args.verbose else logging.WARNING)
    socketio_log.propagate = False
    #Transaction log
    tx_logger = logging.getLogger("transaction_log") #get transaction logger
    tx_logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    if os.name == 'nt':
        tx_fileh = util_windows.SanitizedRotatingFileHandler(config.TX_LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
    else:
        tx_fileh = logging.handlers.RotatingFileHandler(config.TX_LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
    tx_fileh.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    tx_formatter = logging.Formatter('%(asctime)s %(message)s', '%Y-%m-%d-T%H:%M:%S%z')
    tx_fileh.setFormatter(tx_formatter)
    tx_logger.addHandler(tx_fileh)
    tx_logger.propagate = False
    
    logging.info("counterblock Version %s starting ..." % config.VERSION)
    
    #Load in counterwallet config settings
    #TODO: Hardcode in cw path for now. Will be taken out to a plugin shortly...
    counterwallet_config_path = os.path.join('/home/xcp/counterwallet/counterwallet.conf.json')
    if os.path.exists(counterwallet_config_path):
        logging.info("Loading counterwallet config at '%s'" % counterwallet_config_path)
        with open(counterwallet_config_path) as f:
            config.COUNTERWALLET_CONFIG_JSON = f.read()
    else:
        logging.warn("Counterwallet config does not exist at '%s'" % counterwallet_config_path)
        config.COUNTERWALLET_CONFIG_JSON = '{}'
    try:
        config.COUNTERWALLET_CONFIG = json.loads(config.COUNTERWALLET_CONFIG_JSON)
    except Exception, e:
        logging.error("Exception loading counterwallet config: %s" % e)
    
    #Module Setup
    def module_setup(module_path): 
        logging.debug('Loading Module %s' %module_path)
        f, fl, dsc = imp.find_module(module_path)
        module_name = module_path.split('/')[-1]
        imp.load_module(module_name, f, fl, dsc) 
        logging.info('Module Loaded %s' %module_name)
        
    def get_mod_params_dict(params):
        if not isinstance(params, list): params = [params] 
        params_dict = {} 
        try: 
            params_dict['priority'] = float(params[0])
        except: params_dict['enabled'] = False if "false" == params[0].lower() else True
        if len(params) > 1: 
            try: params_dict['priority'] = float(params[1]) 
            except: params_dict['enabled'] = False if "false" == params[1].lower() else True
        return params_dict

    #Read counterblockd_module.conf
    #Moved to after logging config
    module_conf = ConfigObj(os.path.join(config.DATA_DIR, 'counterblockd_module.conf'))
    for key, container in module_conf.items():
        if key == 'LoadModule':
            for module, user_settings in container.items(): 
                try:
                    params = get_mod_params_dict(user_settings)
                    if params['enabled'] is True: module_setup(module) 
                except: logging.warn("Failed to load Module %s" %module)
        elif 'Processor' in key:
            try: processor_functions = processor.__dict__[key]
            except: 
                logging.warn("Invalid config header %s in counterblockd_module.conf" %key)
                continue
            #print(processor_functions)
            for func_name, user_settings in container.items(): 
                #print(func_name, user_settings)
                if func_name in processor_functions:
                    params = get_mod_params_dict(user_settings)
                    #print(func_name, params)
                    for param_name, param_value in params.items(): 
                        processor_functions[func_name][param_name] = param_value
                else:
                    logging.warn("Attempted to configure a non-existent processor %s" %func_name)
            logging.debug(processor_functions)
            
    #xnova(7/16/2014): Disable for now, as this uses requests under the surface, which may not be safe for a gevent-based app
    #rollbar integration
    #if config.ROLLBAR_TOKEN:
    #    logging.info("Rollbar support enabled. Logging for environment: %s" % config.ROLLBAR_ENV)
    #    rollbar.init(config.ROLLBAR_TOKEN, config.ROLLBAR_ENV, allow_logging_basic_config=False)
    #    
    #    def report_errors(ex_cls, ex, tb):
    #        rollbar.report_exc_info((ex_cls, ex, tb))
    #        raise ex #re-raise
    #    sys.excepthook = report_errors

    # GeoIP
    config.GEOIP = util.init_geoip()

    #Connect to mongodb
    logging.info("Connecting to mongoDB backend ...")
    mongo_client = pymongo.MongoClient(config.MONGODB_CONNECT, config.MONGODB_PORT)
    mongo_db = mongo_client[config.MONGODB_DATABASE] #will create if it doesn't exist
    if config.MONGODB_USER and config.MONGODB_PASSWORD:
        if not mongo_db.authenticate(config.MONGODB_USER, config.MONGODB_PASSWORD):
            raise Exception("Could not authenticate to mongodb with the supplied username and password.")
    config.mongo_db = mongo_db #should be able to access fine across greenlets, etc

    #insert mongo indexes if need-be (i.e. for newly created database)
    ##COLLECTIONS THAT ARE PURGED AS A RESULT OF A REPARSE
    #processed_blocks
    mongo_db.processed_blocks.ensure_index('block_index', unique=True)
    #tracked_assets
    mongo_db.tracked_assets.ensure_index('asset', unique=True)
    mongo_db.tracked_assets.ensure_index('_at_block') #for tracked asset pruning
    mongo_db.tracked_assets.ensure_index([
        ("owner", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
    ])
    #trades
    mongo_db.trades.ensure_index([
        ("base_asset", pymongo.ASCENDING),
        ("quote_asset", pymongo.ASCENDING),
        ("block_time", pymongo.DESCENDING)
    ])
    mongo_db.trades.ensure_index([ #events.py and elsewhere (for singlular block_index index access)
        ("block_index", pymongo.ASCENDING),
        ("base_asset", pymongo.ASCENDING),
        ("quote_asset", pymongo.ASCENDING)
    ])

    #balance_changes
    mongo_db.balance_changes.ensure_index('block_index')
    mongo_db.balance_changes.ensure_index([
        ("address", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
        ("block_time", pymongo.ASCENDING)
    ])
    #asset_market_info
    mongo_db.asset_market_info.ensure_index('asset', unique=True)
    #asset_marketcap_history
    mongo_db.asset_marketcap_history.ensure_index('block_index')
    mongo_db.asset_marketcap_history.ensure_index([ #events.py
        ("market_cap_as", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
        ("block_index", pymongo.DESCENDING)
    ])
    mongo_db.asset_marketcap_history.ensure_index([ #api.py
        ("market_cap_as", pymongo.ASCENDING),
        ("block_time", pymongo.DESCENDING)
    ])
    #asset_pair_market_info
    mongo_db.asset_pair_market_info.ensure_index([ #event.py, api.py
        ("base_asset", pymongo.ASCENDING),
        ("quote_asset", pymongo.ASCENDING)
    ], unique=True)
    mongo_db.asset_pair_market_info.ensure_index('last_updated')
    #asset_extended_info
    mongo_db.asset_extended_info.ensure_index('asset', unique=True)
    mongo_db.asset_extended_info.ensure_index('info_status')
    #btc_open_orders
    mongo_db.btc_open_orders.ensure_index('when_created')
    mongo_db.btc_open_orders.ensure_index('order_tx_hash', unique=True)
    #transaction_stats
    mongo_db.transaction_stats.ensure_index([ #blockfeed.py, api.py
        ("when", pymongo.ASCENDING),
        ("category", pymongo.DESCENDING)
    ])
    mongo_db.transaction_stats.ensure_index('message_index', unique=True)
    mongo_db.transaction_stats.ensure_index('block_index')
    #wallet_stats
    mongo_db.wallet_stats.ensure_index([
        ("when", pymongo.ASCENDING),
        ("network", pymongo.ASCENDING),
    ])
    
    ##COLLECTIONS THAT ARE *NOT* PURGED AS A RESULT OF A REPARSE
    #preferences
    mongo_db.preferences.ensure_index('wallet_id', unique=True)
    mongo_db.preferences.ensure_index('network')
    mongo_db.preferences.ensure_index('last_touched')
    #login_history
    mongo_db.login_history.ensure_index('wallet_id')
    mongo_db.login_history.ensure_index([
        ("when", pymongo.DESCENDING),
        ("network", pymongo.ASCENDING),
        ("action", pymongo.ASCENDING),
    ])
    #chat_handles
    mongo_db.chat_handles.ensure_index('wallet_id', unique=True)
    mongo_db.chat_handles.ensure_index('handle', unique=True)
    #chat_history
    mongo_db.chat_history.ensure_index('when')
    mongo_db.chat_history.ensure_index([
        ("handle", pymongo.ASCENDING),
        ("when", pymongo.DESCENDING),
    ])
    #feeds
    mongo_db.feeds.ensure_index('source')
    mongo_db.feeds.ensure_index('owner')
    mongo_db.feeds.ensure_index('category')
    mongo_db.feeds.ensure_index('info_url')
    #mempool
    mongo_db.mempool.ensure_index('tx_hash')
    
    #Connect to redis
    if config.REDIS_ENABLE_APICACHE:
        logging.info("Enabling redis read API caching... (%s:%s)" % (config.REDIS_CONNECT, config.REDIS_PORT))
        redis_client = redis.StrictRedis(host=config.REDIS_CONNECT, port=config.REDIS_PORT, db=config.REDIS_DATABASE)
        config.REDIS_CLIENT = redis_client
    else:
        redis_client = None
        config.REDIS_CLIENT = None
    
    #set up zeromq publisher for sending out received events to connected socket.io clients
    zmq_context = zmq.Context()
    zmq_publisher_eventfeed = zmq_context.socket(zmq.PUB)
    zmq_publisher_eventfeed.bind('inproc://queue_eventfeed')
    #set event feed for shared access
    config.ZMQ_PUBLISHER_EVENTFEED = zmq_publisher_eventfeed

    logging.info("Starting up socket.io server (block event feed)...")
    sio_server = socketio_server.SocketIOServer(
        (config.SOCKETIO_HOST, config.SOCKETIO_PORT),
        siofeeds.SocketIOMessagesFeedServer(zmq_context),
        resource="socket.io", policy_server=False)
    sio_server.start() #start the socket.io server greenlets

    logging.info("Starting up socket.io server (chat feed)...")
    sio_server = socketio_server.SocketIOServer(
        (config.SOCKETIO_CHAT_HOST, config.SOCKETIO_CHAT_PORT),
        siofeeds.SocketIOChatFeedServer(mongo_db),
        resource="socket.io", policy_server=False)
    sio_server.start() #start the socket.io server greenlets
    
    #Run Startup Functions
    processor.StartUpProcessor.run_active_functions()


    #start up event timers that don't depend on the feed being fully caught up

    if not config.SUPPORT_EMAIL:
        logging.warn("Support email setting not set: To enable, please specify an email for the 'support-email' setting in your counterblockd.conf")


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
