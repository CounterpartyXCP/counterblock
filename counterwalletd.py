#! /usr/bin/env python3
"""
counterwalletd server
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

import appdirs
import pymongo
import zmq.green as zmq
import rollbar
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

    parser.add_argument('--reparse', action='store_true', default=False, help='force full re-initialization of the counterwalletd database')
    parser.add_argument('--testnet', action='store_true', default=False, help='use Bitcoin testnet addresses and block numbers')
    parser.add_argument('--data-dir', help='specify to explicitly override the directory in which to keep the config file and log file')
    parser.add_argument('--config-file', help='the location of the configuration file')
    parser.add_argument('--log-file', help='the location of the log file')
    parser.add_argument('--tx-log-file', help='the location of the transaction log file')
    parser.add_argument('--pid-file', help='the location of the pid file')

    #THINGS WE CONNECT TO
    parser.add_argument('--counterpartyd-rpc-connect', help='the hostname of the counterpartyd JSON-RPC server')
    parser.add_argument('--counterpartyd-rpc-port', type=int, help='the port used to communicate with counterpartyd over JSON-RPC')
    parser.add_argument('--counterpartyd-rpc-user', help='the username used to communicate with counterpartyd over JSON-RPC')
    parser.add_argument('--counterpartyd-rpc-password', help='the password used to communicate with counterpartyd over JSON-RPC')

    parser.add_argument('--insight-connect', help='the insight server hostname or IP to connect to')
    parser.add_argument('--insight-port', type=int, help='the insight server port to connect to')

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

    #THINGS WE HOST
    parser.add_argument('--rpc-host', help='the IP of the interface to bind to for providing JSON-RPC API access (0.0.0.0 for all interfaces)')
    parser.add_argument('--rpc-port', type=int, help='port on which to provide the counterwalletd JSON-RPC API')
    parser.add_argument('--socketio-host', help='the interface on which to host the counterwalletd socket.io API')
    parser.add_argument('--socketio-port', type=int, help='port on which to provide the counterwalletd socket.io API')
    parser.add_argument('--socketio-chat-host', help='the interface on which to host the counterwalletd socket.io chat API')
    parser.add_argument('--socketio-chat-port', type=int, help='port on which to provide the counterwalletd socket.io chat API')

    parser.add_argument('--allow-cors', action='store_true', default=False, help='Allow ajax cross domain request')
    
    parser.add_argument('--rollbar-token', help='the API token to use with rollbar (leave blank to disable rollbar integration)')
    parser.add_argument('--rollbar-env', help='the environment name for the rollbar integration (if enabled). Defaults to \'production\'')

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
        
    # reparse
    config.REPARSE_FORCED = args.reparse
        
    ##############
    # THINGS WE CONNECT TO

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
    config.COUNTERPARTYD_AUTH = HTTPBasicAuth(config.COUNTERPARTYD_RPC_USER, config.COUNTERPARTYD_RPC_PASSWORD) if (config.COUNTERPARTYD_RPC_USER and config.COUNTERPARTYD_RPC_PASSWORD) else None

    # insight API host
    if args.insight_connect:
        config.INSIGHT_CONNECT = args.insight_connect
    elif has_config and configfile.has_option('Default', 'insight-connect') and configfile.get('Default', 'insight-connect'):
        config.INSIGHT_CONNECT = configfile.get('Default', 'insight-connect')
    else:
        config.INSIGHT_CONNECT = 'localhost'

    # insight API port
    if args.insight_port:
        config.INSIGHT_PORT = args.insight_port
    elif has_config and configfile.has_option('Default', 'insight-port') and configfile.get('Default', 'insight-port'):
        config.INSIGHT_PORT = configfile.get('Default', 'insight-port')
    else:
        if config.TESTNET:
            config.INSIGHT_PORT = 3001
        else:
            config.INSIGHT_PORT = 3000
    try:
        config.INSIGHT_PORT = int(config.INSIGHT_PORT)
        assert int(config.INSIGHT_PORT) > 1 and int(config.INSIGHT_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number insight-port configuration parameter")

    config.INSIGHT = 'http://' + config.INSIGHT_CONNECT + ':' + str(config.INSIGHT_PORT)

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
        config.BLOCK_FIRST = 154908
    else:
        config.BLOCK_FIRST = 278270

    # Log
    if args.log_file:
        config.LOG = args.log_file
    elif has_config and configfile.has_option('Default', 'log-file'):
        config.LOG = configfile.get('Default', 'log-file')
    else:
        config.LOG = os.path.join(config.data_dir, 'counterwalletd.log')
        
    if args.tx_log_file:
        config.TX_LOG = args.tx_log_file
    elif has_config and configfile.has_option('Default', 'tx-log-file'):
        config.TX_LOG = configfile.get('Default', 'tx-log-file')
    else:
        config.TX_LOG = os.path.join(config.data_dir, 'counterwalletd-tx.log')
    

    # PID
    if args.pid_file:
        config.PID = args.pid_file
    elif has_config and configfile.has_option('Default', 'pid-file'):
        config.PID = configfile.get('Default', 'pid-file')
    else:
        config.PID = os.path.join(config.data_dir, 'counterwalletd.pid')

     # CORS
    if args.allow_cors:
        config.ALLOW_CORS = args.allow_cors
    elif has_config and configfile.has_option('Default', 'allow-cors'):
        config.ALLOW_CORS = configfile.getboolean('Default', 'allow-cors')
    else:
        config.ALLOW_CORS = True
    
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
        config.ROLLBAR_ENV = 'counterwalletd-production'

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
    #API requests logging (don't show on console in normal operation)
    requests_log = logging.getLogger("requests")
    requests_log.setLevel(logging.DEBUG if args.verbose else logging.WARNING)
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
    
    #rollbar integration
    if config.ROLLBAR_TOKEN:
        logging.info("Rollbar support enabled. Logging for environment: %s" % config.ROLLBAR_ENV)
        rollbar.init(config.ROLLBAR_TOKEN, config.ROLLBAR_ENV, allow_logging_basic_config=False)
        
        def report_errors(ex_cls, ex, tb):
            rollbar.report_exc_info((ex_cls, ex, tb))
            raise ex #re-raise
        sys.excepthook = report_errors

    #Connect to mongodb
    mongo_client = pymongo.MongoClient(config.MONGODB_CONNECT, config.MONGODB_PORT)
    mongo_db = mongo_client[config.MONGODB_DATABASE] #will create if it doesn't exist
    if config.MONGODB_USER and config.MONGODB_PASSWORD:
        if not mongo_db.authenticate(config.MONGODB_USER, config.MONGODB_PASSWORD):
            raise Exception("Could not authenticate to mongodb with the supplied username and password.")

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
    #btc_open_orders
    mongo_db.btc_open_orders.ensure_index('when_created')
    mongo_db.btc_open_orders.ensure_index('order_tx_hash', unique=True)
    #asset_extended_info
    mongo_db.asset_extended_info.ensure_index('asset', unique=True)
    
    ##COLLECTIONS THAT ARE *NOT* PURGED AS A RESULT OF A REPARSE
    #preferences
    mongo_db.preferences.ensure_index('wallet_id', unique=True)
    mongo_db.preferences.ensure_index('last_touched')
    #chat_handles
    mongo_db.chat_handles.ensure_index('wallet_id', unique=True)
    mongo_db.chat_handles.ensure_index('handle', unique=True)
    #chat_history
    mongo_db.chat_history.ensure_index('when')
    mongo_db.chat_history.ensure_index([
        ("handle", pymongo.ASCENDING),
        ("when", pymongo.DESCENDING),
    ])
    
    #Connect to redis
    if config.REDIS_ENABLE_APICACHE:
        logging.info("Enabling redis read API caching... (%s:%s)" % (config.REDIS_CONNECT, config.REDIS_PORT))
        redis_client = redis.StrictRedis(host=config.REDIS_CONNECT, port=config.REDIS_PORT, db=config.REDIS_DATABASE)
    else:
        redis_client = None
    
    #set up zeromq publisher for sending out received events to connected socket.io clients
    zmq_context = zmq.Context()
    zmq_publisher_eventfeed = zmq_context.socket(zmq.PUB)
    zmq_publisher_eventfeed.bind('inproc://queue_eventfeed')

    logging.info("Starting up socket.io server (block event feed)...")
    sio_server = socketio_server.SocketIOServer(
        (config.SOCKETIO_HOST, config.SOCKETIO_PORT),
        siofeeds.SocketIOMessagesFeedServer(zmq_context),
        resource="socket.io", policy_server=False)
    sio_server.start() #start the socket.io server greenlets

    logging.info("Starting up socket.io server (counterwallet chat)...")
    sio_server = socketio_server.SocketIOServer(
        (config.SOCKETIO_CHAT_HOST, config.SOCKETIO_CHAT_PORT),
        siofeeds.SocketIOChatFeedServer(mongo_db),
        resource="socket.io", policy_server=False)
    sio_server.start() #start the socket.io server greenlets

    logging.info("Starting up counterpartyd block feed poller...")
    gevent.spawn(blockfeed.process_cpd_blockfeed, mongo_db, zmq_publisher_eventfeed)

    #start up event timers that don't depend on the feed being fully caught up
    logging.debug("Starting event timer: expire_stale_prefs")
    gevent.spawn(events.expire_stale_prefs, mongo_db)
    logging.debug("Starting event timer: expire_stale_btc_open_order_records")
    gevent.spawn(events.expire_stale_btc_open_order_records, mongo_db)

    logging.info("Starting up RPC API handler...")
    api.serve_api(mongo_db, redis_client)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
