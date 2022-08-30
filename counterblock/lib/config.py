# -*- coding: utf-8 -*-

##
# CONSTANTS
##
VERSION = "1.4.0"  # should keep up with counterblockd repo's release tag

DB_VERSION = 24  # a db version increment will cause counterblockd to rebuild its database off of counterpartyd

UNIT = 100000000

MARKET_PRICE_DERIVE_NUM_POINTS = 8  # number of last trades over which to derive the market price (via WVAP)

# FROM counterpartyd
# NOTE: These constants must match those in counterparty-lib/counterpartylib/lib/config.py
REGULAR_DUST_SIZE = 546
MULTISIG_DUST_SIZE = 7800
ORDER_BTC_DUST_LIMIT_CUTOFF = MULTISIG_DUST_SIZE

BTC = 'BTC'
XBTC = 'XBTC'
XCP = 'XCP'

BTC_NAME = "Bitcoin"
XCP_NAME = "Counterparty"
APP_NAME = "counterblock"
COUNTERPARTY_APP_NAME = XCP_NAME.lower()

BTC_TO_XCP = BTC + '/' + XCP
XCP_TO_BTC = XCP + '/' + BTC

MAX_REORG_NUM_BLOCKS = 10  # max reorg we'd likely ever see
MAX_FORCED_REORG_NUM_BLOCKS = 20  # but let us go deeper when messages are out of sync

QUOTE_ASSETS = [BTC, XBTC, XCP, 'PEPECASH', 'BITCRYSTALS', 'FLDC', 'RUSTBITS', 'SCOTCOIN', 'DATABITS', 'BITCORN', 'MAFIACASH']  # define the priority for quote asset
MARKET_LIST_QUOTE_ASSETS = [XCP, BTC]  # define the order in the market list

DEFAULT_BACKEND_PORT_REGTEST = 28332
DEFAULT_BACKEND_PORT_TESTNET = 18332
DEFAULT_BACKEND_PORT = 8332

DEFAULT_LOG_SIZE_KB = 20000
DEFAULT_LOG_NUM_FILES = 5

##
# STATE
##
mongo_db = None  # will be set on server init
state = {
    'caught_up': False  # atomic state variable, set to True when counterpartyd AND counterblockd are caught up
    # the rest of this is added dynamically
}


##
# METHODS
##
def get_dirs():
    import appdirs
    data_dir = appdirs.user_data_dir(appauthor=XCP_NAME, appname=APP_NAME, roaming=True)
    config_dir = appdirs.user_config_dir(appauthor=XCP_NAME, appname=APP_NAME, roaming=True)
    log_dir = appdirs.user_log_dir(appauthor=XCP_NAME, appname=APP_NAME)
    return data_dir, config_dir, log_dir


def init_base(args):
    import os
    import configparser
    import email.utils

    # testnet
    global TESTNET
    if args.testnet:
        TESTNET = args.testnet
    else:
        TESTNET = False

    global REGTEST
    if args.regtest:
        REGTEST = args.regtest
    else:
        REGTEST = False

    global net_path_part
    if TESTNET:
        net_path_part = '.testnet'
    elif REGTEST:
        net_path_part = '.regtest'
    else:
        net_path_part = ''

    # first block
    global BLOCK_FIRST
    if TESTNET:
        BLOCK_FIRST = 310000
    elif REGTEST:
        BLOCK_FIRST = 100
    else:
        BLOCK_FIRST = 278270

    global LATEST_BLOCK_INIT
    LATEST_BLOCK_INIT = {'block_index': BLOCK_FIRST, 'block_time': None, 'block_hash': None}

    # init variables used for reparse operations
    global IS_REPARSING
    IS_REPARSING = False
    global QUIT_AFTER_CAUGHT_UP
    QUIT_AFTER_CAUGHT_UP = False

    ##############
    # THINGS WE CONNECT TO

    # backend (e.g. bitcoind)
    global BACKEND_CONNECT
    if args.backend_connect:
        BACKEND_CONNECT = args.backend_connect
    else:
        BACKEND_CONNECT = 'localhost'

    global BACKEND_PORT
    if args.backend_port:
        BACKEND_PORT = args.backend_port
    else:
        if TESTNET:
            BACKEND_PORT = DEFAULT_BACKEND_PORT_TESTNET
        elif REGTEST:
            BACKEND_PORT = DEFAULT_BACKEND_PORT_REGTEST
        else:
            BACKEND_PORT = DEFAULT_BACKEND_PORT
    try:
        BACKEND_PORT = int(BACKEND_PORT)
        assert int(BACKEND_PORT) > 1 and int(BACKEND_PORT) < 65535
    except:
        raise Exception("Please specify a valid port number for the backend-port configuration parameter")

    global BACKEND_USER
    if args.backend_user:
        BACKEND_USER = args.backend_user
    else:
        BACKEND_USER = 'rpc'

    global BACKEND_PASSWORD
    if args.backend_password:
        BACKEND_PASSWORD = args.backend_password
    else:
        BACKEND_PASSWORD = 'rpcpassword'

    global BACKEND_AUTH
    BACKEND_AUTH = (BACKEND_USER, BACKEND_PASSWORD) if (BACKEND_USER and BACKEND_PASSWORD) else None

    global BACKEND_URL
    BACKEND_URL = 'http://' + BACKEND_USER + ':' + BACKEND_PASSWORD + '@' + BACKEND_CONNECT + ':' + str(BACKEND_PORT)

    global BACKEND_URL_NOAUTH
    BACKEND_URL_NOAUTH = 'http://' + BACKEND_CONNECT + ':' + str(BACKEND_PORT) + '/'

    # counterpartyd RPC connection
    global COUNTERPARTY_CONNECT
    if args.counterparty_connect:
        COUNTERPARTY_CONNECT = args.counterparty_connect
    else:
        COUNTERPARTY_CONNECT = 'localhost'

    global COUNTERPARTY_PORT
    if args.counterparty_port:
        COUNTERPARTY_PORT = args.counterparty_port
    else:
        if TESTNET:
            COUNTERPARTY_PORT = 14000
        elif REGTEST:
            COUNTERPARTY_PORT = 24000
        else:
            COUNTERPARTY_PORT = 4000
    try:
        COUNTERPARTY_PORT = int(COUNTERPARTY_PORT)
        assert int(COUNTERPARTY_PORT) > 1 and int(COUNTERPARTY_PORT) < 65535
    except:
        raise Exception("Please specify a valid port number for the counterparty-port configuration parameter")

    global COUNTERPARTY_USER
    if args.counterparty_user:
        COUNTERPARTY_USER = args.counterparty_user
    else:
        COUNTERPARTY_USER = 'rpc'

    global COUNTERPARTY_PASSWORD
    if args.counterparty_password:
        COUNTERPARTY_PASSWORD = args.counterparty_password
    else:
        COUNTERPARTY_PASSWORD = 'rpcpassword'

    global COUNTERPARTY_RPC
    COUNTERPARTY_RPC = 'http://' + COUNTERPARTY_CONNECT + ':' + str(COUNTERPARTY_PORT) + '/api/'

    global COUNTERPARTY_AUTH
    COUNTERPARTY_AUTH = (COUNTERPARTY_USER, COUNTERPARTY_PASSWORD) if (COUNTERPARTY_USER and COUNTERPARTY_PASSWORD) else None

    # mongodb
    global MONGODB_CONNECT
    if args.mongodb_connect:
        MONGODB_CONNECT = args.mongodb_connect
    else:
        MONGODB_CONNECT = 'localhost'

    global MONGODB_PORT
    if args.mongodb_port:
        MONGODB_PORT = args.mongodb_port
    else:
        MONGODB_PORT = 27017
    try:
        MONGODB_PORT = int(MONGODB_PORT)
        assert int(MONGODB_PORT) > 1 and int(MONGODB_PORT) < 65535
    except:
        raise Exception("Please specify a valid port number for the mongodb-port configuration parameter")

    global MONGODB_DATABASE
    if args.mongodb_database:
        MONGODB_DATABASE = args.mongodb_database
    else:
        if TESTNET:
            MONGODB_DATABASE = 'counterblockd_testnet'
        elif REGTEST:
            MONGODB_DATABASE = 'counterblockd_regtest'
        else:
            MONGODB_DATABASE = 'counterblockd'

    global MONGODB_USER
    if args.mongodb_user:
        MONGODB_USER = args.mongodb_user
    else:
        MONGODB_USER = None

    global MONGODB_PASSWORD
    if args.mongodb_password:
        MONGODB_PASSWORD = args.mongodb_password
    else:
        MONGODB_PASSWORD = None

    # redis-related
    global REDIS_CONNECT
    if args.redis_connect:
        REDIS_CONNECT = args.redis_connect
    else:
        REDIS_CONNECT = '127.0.0.1'

    global REDIS_PORT
    if args.redis_port:
        REDIS_PORT = args.redis_port
    else:
        REDIS_PORT = 6379
    try:
        REDIS_PORT = int(REDIS_PORT)
        assert int(REDIS_PORT) > 1 and int(REDIS_PORT) < 65535
    except:
        raise Exception("Please specify a valid port number for the redis-port configuration parameter")

    global REDIS_DATABASE
    if args.redis_database:
        REDIS_DATABASE = args.redis_database
    else:
        if TESTNET:
            REDIS_DATABASE = 1
        elif REGTEST:
            REDIS_DATABASE = 2
        else:
            REDIS_DATABASE = 0
    try:
        REDIS_DATABASE = int(REDIS_DATABASE)
        assert int(REDIS_DATABASE) >= 0 and int(REDIS_DATABASE) <= 16
    except:
        raise Exception("Please specify a valid redis-database configuration parameter (between 0 and 16 inclusive)")

    global BLOCKTRAIL_API_KEY
    BLOCKTRAIL_API_KEY = args.blocktrail_api_key or None
    global BLOCKTRAIL_API_SECRET
    BLOCKTRAIL_API_SECRET = args.blocktrail_api_secret or None

    ##############
    # THINGS WE SERVE

    global RPC_HOST
    if args.rpc_host:
        RPC_HOST = args.rpc_host
    else:
        RPC_HOST = 'localhost'

    global RPC_PORT
    if args.rpc_port:
        RPC_PORT = args.rpc_port
    else:
        if TESTNET:
            RPC_PORT = 14100
        elif REGTEST:
            RPC_PORT = 24100
        else:
            RPC_PORT = 4100
    try:
        RPC_PORT = int(RPC_PORT)
        assert int(RPC_PORT) > 1 and int(RPC_PORT) < 65535
    except:
        raise Exception("Please specify a valid port number for the rpc-port configuration parameter")

    global RPC_ALLOW_CORS
    if args.rpc_allow_cors:
        RPC_ALLOW_CORS = args.rpc_allow_cors
    else:
        RPC_ALLOW_CORS = True

    # Other things
    global SUBDIR_ASSET_IMAGES
    SUBDIR_ASSET_IMAGES = "asset_img%s" % net_path_part  # goes under the data dir and stores retrieved asset images
    global SUBDIR_FEED_IMAGES
    SUBDIR_FEED_IMAGES = "feed_img%s" % net_path_part  # goes under the data dir and stores retrieved feed images

    ##############
    # OTHER SETTINGS

    # System (logging, pids, etc)
    global COUNTERBLOCKD_DIR
    COUNTERBLOCKD_DIR = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))

    global LOG
    if args.log_file is False:  # no file logging
        LOG = None
    elif not args.log_file:  # default location
        LOG = os.path.join(log_dir, 'server%s.log' % net_path_part)
    else:  # user-specified location
        LOG = args.log_file

    global LOG_SIZE_KB
    if args.log_size_kb:
        LOG_SIZE_KB = args.log_size_kb
    else:
        LOG_SIZE_KB = DEFAULT_LOG_SIZE_KB
    try:
        LOG_SIZE_KB = int(LOG_SIZE_KB)
        assert LOG_SIZE_KB > 0
    except:
        raise Exception("Please specify a valid log-size-kb value (in kilobytes)")

    global LOG_NUM_FILES
    if args.log_num_files:
        LOG_NUM_FILES = args.log_num_files
    else:
        LOG_NUM_FILES = DEFAULT_LOG_NUM_FILES
    try:
        LOG_NUM_FILES = int(LOG_NUM_FILES)
        assert LOG_NUM_FILES > 0 and LOG_NUM_FILES <= 100
    except:
        raise Exception("Please specify a valid log-num-files value (must be less than 100)")

    global TX_LOG
    if args.tx_log_file is False:  # no file logging
        TX_LOG = None
    elif not args.tx_log_file:  # default location
        TX_LOG = os.path.join(log_dir, 'server%s.tx.log' % net_path_part)
    else:  # user-specified location
        TX_LOG = args.tx_log_file

    global PID
    if args.pid_file:
        PID = args.pid_file
    else:
        PID = os.path.join(data_dir, 'server%s.pid' % net_path_part)


def load_schemas():
    """initialize json schema for json asset and feed validation"""
    import os
    import json
    assert COUNTERBLOCKD_DIR

    global ASSET_SCHEMA
    ASSET_SCHEMA = json.load(open(os.path.join(COUNTERBLOCKD_DIR, 'schemas', 'asset.schema.json')))

    global FEED_SCHEMA
    FEED_SCHEMA = json.load(open(os.path.join(COUNTERBLOCKD_DIR, 'schemas', 'feed.schema.json')))


def init(args):
    #set up dirs
    global data_dir, config_dir, log_dir
    data_dir, config_dir, log_dir = get_dirs()
    assert data_dir and config_dir and log_dir

    init_base(args)
    load_schemas()
