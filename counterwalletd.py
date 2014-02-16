#! /usr/bin/env python3
"""
counterwalletd server
"""

#import before importing other modules
import gevent
from gevent import monkey; monkey.patch_all()

import os
import argparse
import json
import logging
import datetime
import ConfigParser
import time

import appdirs
import pymongo
import redis
import redis.connection
redis.connection.socket = gevent.socket #make redis play well with gevent

from socketio import server as socketio_server
from requests.auth import HTTPBasicAuth

from lib import (config, api, events, blockfeed, siofeeds, util)

if __name__ == '__main__':
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(prog='counterwalletd', description='Counterwallet daemon. Works with counterpartyd')
    parser.add_argument('-V', '--version', action='version', version="counterwalletd v%s" % config.VERSION)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='sets log level to DEBUG instead of WARNING')

    parser.add_argument('--reinit', action='store_true', default=False, help='force full re-initialization of the counterwalletd database')
    parser.add_argument('--testnet', action='store_true', default=False, help='use Bitcoin testnet addresses and block numbers')
    parser.add_argument('--data-dir', help='specify to explicitly override the directory in which to keep the config file and log file')
    parser.add_argument('--config-file', help='the location of the configuration file')
    parser.add_argument('--log-file', help='the location of the log file')

    #STUFF WE CONNECT TO
    parser.add_argument('--bitcoind-rpc-connect', help='the hostname of the Bitcoind JSON-RPC server')
    parser.add_argument('--bitcoind-rpc-port', type=int, help='the port used to communicate with Bitcoind over JSON-RPC')
    parser.add_argument('--bitcoind-rpc-user', help='the username used to communicate with Bitcoind over JSON-RPC')
    parser.add_argument('--bitcoind-rpc-password', help='the password used to communicate with Bitcoind over JSON-RPC')

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

    parser.add_argument('--cube-connect', help='the hostname of the Square Cube collector + evaluator')
    parser.add_argument('--cube-collector-port', type=int, help='the port used to communicate with the Square Cube collector')
    parser.add_argument('--cube-evaluator-port', type=int, help='the port used to communicate with the Square Cube evaluator')
    parser.add_argument('--cube-database', help='the name of the mongo database cube stores its data within')

    #STUFF WE HOST
    parser.add_argument('--rpc-host', help='the interface on which to host the counterwalletd JSON-RPC API')
    parser.add_argument('--rpc-port', type=int, help='port on which to provide the counterwalletd JSON-RPC API')
    parser.add_argument('--socketio-host', help='the interface on which to host the counterwalletd socket.io API')
    parser.add_argument('--socketio-port', type=int, help='port on which to provide the counterwalletd socket.io API')
    parser.add_argument('--socketio-chat-host', help='the interface on which to host the counterwalletd socket.io chat API')
    parser.add_argument('--socketio-chat-port', type=int, help='port on which to provide the counterwalletd socket.io chat API')

    args = parser.parse_args()

    # Data directory
    if not args.data_dir:
        config.data_dir = appdirs.user_data_dir(appauthor='Counterparty', appname='counterwalletd', roaming=True)
    else:
        config.data_dir = args.data_dir
    if not os.path.isdir(config.data_dir): os.mkdir(config.data_dir)

    #Read config file
    configfile = ConfigParser.ConfigParser()
    config_path = os.path.join(config.data_dir, 'counterwalletd.conf')
    configfile.read(config_path)
    has_config = configfile.has_section('Default')

    # testnet
    if args.testnet:
        config.TESTNET = args.testnet
    elif has_config and configfile.has_option('Default', 'testnet'):
        config.TESTNET = configfile.getboolean('Default', 'testnet')
    else:
        config.TESTNET = False
        
    # reinit
    config.REINIT_FORCED = args.reinit
        
    ##############
    # STUFF WE CONNECT TO

    # Bitcoind RPC host
    if args.bitcoind_rpc_connect:
        config.BITCOIND_RPC_CONNECT = args.bitcoind_rpc_connect
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-connect') and configfile.get('Default', 'bitcoind-rpc-connect'):
        config.BITCOIND_RPC_CONNECT = configfile.get('Default', 'bitcoind-rpc-connect')
    else:
        config.BITCOIND_RPC_CONNECT = 'localhost'

    # Bitcoind RPC port
    if args.bitcoind_rpc_port:
        config.BITCOIND_RPC_PORT = args.bitcoind_rpc_port
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-port') and configfile.get('Default', 'bitcoind-rpc-port'):
        config.BITCOIND_RPC_PORT = configfile.get('Default', 'bitcoind-rpc-port')
    else:
        if config.TESTNET:
            config.BITCOIND_RPC_PORT = '18332'
        else:
            config.BITCOIND_RPC_PORT = '8332'
    try:
        int(config.BITCOIND_RPC_PORT)
        assert int(config.BITCOIND_RPC_PORT) > 1 and int(config.BITCOIND_RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number bitcoind-rpc-port configuration parameter")
            
    # Bitcoind RPC user
    if args.bitcoind_rpc_user:
        config.BITCOIND_RPC_USER = args.bitcoind_rpc_user
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-user') and configfile.get('Default', 'bitcoind-rpc-user'):
        config.BITCOIND_RPC_USER = configfile.get('Default', 'bitcoind-rpc-user')
    else:
        config.BITCOIND_RPC_USER = 'bitcoinrpc'

    # Bitcoind RPC password
    if args.bitcoind_rpc_password:
        config.BITCOIND_RPC_PASSWORD = args.bitcoind_rpc_password
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-password') and configfile.get('Default', 'bitcoind-rpc-password'):
        config.BITCOIND_RPC_PASSWORD = configfile.get('Default', 'bitcoind-rpc-password')
    else:
        raise Exception('bitcoind RPC password not set. (Use configuration file or --bitcoind-rpc-password=PASSWORD)')

    config.BITCOIND_RPC = 'http://' + config.BITCOIND_RPC_CONNECT + ':' + str(config.BITCOIND_RPC_PORT)
    config.BITCOIND_AUTH = HTTPBasicAuth(config.BITCOIND_RPC_USER, config.BITCOIND_RPC_PASSWORD) if (config.BITCOIND_RPC_USER and config.BITCOIND_RPC_PASSWORD) else None

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
        int(config.COUNTERPARTYD_RPC_PORT)
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

    config.COUNTERPARTYD_RPC = 'http://' + config.COUNTERPARTYD_RPC_CONNECT + ':' + str(config.COUNTERPARTYD_RPC_PORT)
    config.COUNTERPARTYD_AUTH = HTTPBasicAuth(config.COUNTERPARTYD_RPC_USER, config.COUNTERPARTYD_RPC_PASSWORD) if (config.COUNTERPARTYD_RPC_USER and config.COUNTERPARTYD_RPC_PASSWORD) else None

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
        int(config.MONGODB_PORT)
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
            config.MONGODB_DATABASE = 'counterwalletd_testnet'
        else:
            config.MONGODB_DATABASE = 'counterwalletd'

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
        int(config.REDIS_PORT)
        assert int(config.REDIS_PORT) > 1 and int(config.REDIS_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number redis-port configuration parameter")

    # redis database
    if args.redis_database:
        config.REDIS_DATABASE = args.redis_database
    elif has_config and configfile.has_option('Default', 'redis-database') and configfile.get('Default', 'redis-database'):
        config.REDIS_DATABASE = configfile.get('Default', 'redis-database')
    else:
        config.REDIS_DATABASE = 0
    try:
        int(config.REDIS_DATABASE)
        assert int(config.REDIS_DATABASE) >= 0 and int(config.REDIS_DATABASE) <= 16
    except:
        raise Exception("Please specific a valid redis-database configuration parameter (between 0 and 16 inclusive)")

    ##############
    # STUFF WE SERVE
    
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
        int(config.RPC_PORT)
        assert int(config.RPC_PORT) > 1 and int(config.RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number rpc-port configuration parameter")

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
        int(config.SOCKETIO_PORT)
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
        int(config.SOCKETIO_CHAT_PORT)
        assert int(config.SOCKETIO_CHAT_PORT) > 1 and int(config.SOCKETIO_CHAT_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number socketio-chat-port configuration parameter")

    #More testnet
    if config.TESTNET:
        config.BLOCK_FIRST = 154908
    else:
        config.BLOCK_FIRST = 278270

    # Log
    if args.log_file:
        config.LOG = args.log_file
    elif has_config and configfile.has_option('Default', 'logfile'):
        config.LOG = configfile.get('Default', 'logfile')
    else:
        config.LOG = os.path.join(config.data_dir, 'counterwalletd.log')
    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(filename=config.LOG, level=log_level,
                        format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d-T%H:%M:%S%z')

    # Log also to stderr.
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    requests_log = logging.getLogger("requests")
    requests_log.setLevel(logging.DEBUG if args.verbose else logging.WARNING)

    #Connect to mongodb
    mongo_client = pymongo.MongoClient(config.MONGODB_CONNECT, config.MONGODB_PORT)
    mongo_db = mongo_client[config.MONGODB_DATABASE] #will create if it doesn't exist
    if config.MONGODB_USER and config.MONGODB_PASSWORD:
        if not mongo_db.authenticate(config.MONGODB_USER, config.MONGODB_PASSWORD):
            raise Exception("Could not authenticate to mongodb with the supplied username and password.")

    #insert mongo indexes if need-be (i.e. for newly created database)
    mongo_db.processed_blocks.ensure_index('block_index', unique=True)
    mongo_db.preferences.ensure_index('wallet_id', unique=True)
    mongo_db.preferences.ensure_index('last_touched')
    mongo_db.chat_handles.ensure_index('wallet_id', unique=True)
    mongo_db.chat_handles.ensure_index('handle', unique=True)
    mongo_db.tracked_assets.ensure_index('asset', unique=True)
    mongo_db.tracked_assets.ensure_index('_at_block') #for tracked asset pruning
    mongo_db.tracked_assets.ensure_index([
        ("owner", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
    ])
    mongo_db.trades.ensure_index('block_index')
    mongo_db.trades.ensure_index([
        ("base_asset", pymongo.ASCENDING),
        ("quote_asset", pymongo.ASCENDING),
        ("block_time", pymongo.ASCENDING)
    ])
    mongo_db.balance_changes.ensure_index('block_index')
    mongo_db.balance_changes.ensure_index([
        ("address", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
        ("block_time", pymongo.ASCENDING)
    ])
    
    #Connect to redis
    if config.REDIS_ENABLE_APICACHE:
        logging.info("Enabling redis read API caching... (%s:%s)" % (config.REDIS_CONNECT, config.REDIS_PORT))
        redis_client = redis.StrictRedis(host=config.REDIS_CONNECT, port=config.REDIS_PORT, db=config.REDIS_DATABASE)
    else:
        redis_client = None
    
    to_socketio_queue = gevent.queue.Queue()
    
    logging.info("Starting up socket.io server (block event feed)...")
    sio_server = socketio_server.SocketIOServer(
        (config.SOCKETIO_HOST, config.SOCKETIO_PORT),
        siofeeds.SocketIOEventServer(to_socketio_queue),
        resource="socket.io", policy_server=False)
    sio_server.start() #start the socket.io server greenlets

    logging.info("Starting up socket.io server (counterwallet chat)...")
    sio_server = socketio_server.SocketIOServer(
        (config.SOCKETIO_CHAT_HOST, config.SOCKETIO_CHAT_PORT),
        siofeeds.SocketIOChatServer(mongo_db),
        resource="socket.io", policy_server=False)
    sio_server.start() #start the socket.io server greenlets

    logging.info("Starting up counterpartyd block feed poller...")
    gevent.spawn(blockfeed.process_cpd_blockfeed, mongo_db, to_socketio_queue)

    logging.debug("Starting event timer: expire_stale_prefs")
    gevent.spawn_later(86400, events.expire_stale_prefs)

    logging.info("Starting up RPC API handler...")
    api.serve_api(mongo_db, redis_client)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
