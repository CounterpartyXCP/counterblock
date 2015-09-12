# -*- coding: utf-8 -*-

##
## CONSTANTS
##
VERSION = "1.2.0" #should keep up with counterblockd repo's release tag

DB_VERSION = 23 #a db version increment will cause counterblockd to rebuild its database off of counterpartyd 

UNIT = 100000000

MARKET_PRICE_DERIVE_NUM_POINTS = 8 #number of last trades over which to derive the market price (via WVAP)

# FROM counterpartyd
# NOTE: These constants must match those in counterpartyd/lib/py
REGULAR_DUST_SIZE = 5430
MULTISIG_DUST_SIZE = 5430 * 2
ORDER_BTC_DUST_LIMIT_CUTOFF = MULTISIG_DUST_SIZE

BTC = 'BTC'
XCP = 'XCP'

BTC_NAME = "Bitcoin"
XCP_NAME = "Counterparty"
APP_NAME = "counterblock"

MAX_REORG_NUM_BLOCKS = 10 #max reorg we'd likely ever see
MAX_FORCED_REORG_NUM_BLOCKS = 20 #but let us go deeper when messages are out of sync

QUOTE_ASSETS = ['BTC', 'XBTC', 'XCP'] # define the priority for quote asset
MARKET_LIST_QUOTE_ASSETS = ['XCP', 'XBTC', 'BTC'] # define the order in the market list

DEFAULT_BACKEND_PORT_TESTNET = 18332
DEFAULT_BACKEND_PORT = 8332


##
## STATE
##
mongo_db = None #will be set on server init
state = {
    'caught_up': False #atomic state variable, set to True when counterpartyd AND counterblockd are caught up
    #the rest of this is added dynamically
}


##
## METHODS
##
def init_data_dir(args):
    import os
    import appdirs

    global data_dir
    data_dir = appdirs.user_data_dir(appauthor=XCP_NAME, appname=APP_NAME, roaming=True)
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)

    global config_dir  
    config_dir = appdirs.user_config_dir(appauthor=XCP_NAME, appname=APP_NAME, roaming=True)
    if not os.path.isdir(config_dir):
        os.makedirs(config_dir)

    global log_dir
    log_dir = appdirs.user_log_dir(appauthor=XCP_NAME, appname=APP_NAME)
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
                
def load(args):
    import os
    import ConfigParser
    import email.utils
    
    assert data_dir and config_dir and log_dir
    
    #Read config file
    configfile = ConfigParser.ConfigParser()
    if args.config_file:
        config_path = args.config_file
    else:
        config_path = os.path.join(config_dir, 'server.conf')
    configfile.read(config_path)
    has_config = configfile.has_section('Default')
    
    # testnet
    global TESTNET
    if args.testnet:
        TESTNET = args.testnet
    elif has_config and configfile.has_option('Default', 'testnet'):
        TESTNET = configfile.getboolean('Default', 'testnet')
    else:
        TESTNET = False

    global net_path_part
    net_path_part = '.testnet' if TESTNET else ''
    
    #first block
    global BLOCK_FIRST
    if TESTNET:
        BLOCK_FIRST = 310000
    else:
        BLOCK_FIRST = 278270

    #forced reparse?
    global REPARSE_FORCED
    REPARSE_FORCED = args.reparse
        
    ##############
    # THINGS WE CONNECT TO

    # backend (e.g. bitcoind)
    global BACKEND_CONNECT
    if args.backend_connect:
        BACKEND_CONNECT = args.backend_connect
    elif has_config and configfile.has_option('Default', 'backend-connect') and configfile.get('Default', 'backend-connect'):
        BACKEND_CONNECT = configfile.get('Default', 'backend-connect')
    else:
        BACKEND_CONNECT = 'localhost'

    global BACKEND_PORT
    if args.backend_port:
        BACKEND_PORT = args.backend_port
    elif has_config and configfile.has_option('Default', 'backend-port') and configfile.get('Default', 'backend-port'):
        BACKEND_PORT = configfile.get('Default', 'backend-port')
    else:
        BACKEND_PORT = DEFAULT_BACKEND_PORT_TESTNET if TESTNET else DEFAULT_BACKEND_PORT
    try:
        BACKEND_PORT = int(BACKEND_PORT)
        assert int(BACKEND_PORT) > 1 and int(BACKEND_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number backend-port configuration parameter")
            
    global BACKEND_USER
    if args.backend_user:
        BACKEND_USER = args.backend_user
    elif has_config and configfile.has_option('Default', 'backend-user') and configfile.get('Default', 'backend-user'):
        BACKEND_USER = configfile.get('Default', 'backend-user')
    else:
        BACKEND_USER = 'rpc'

    global BACKEND_PASSWORD
    if args.backend_password:
        BACKEND_PASSWORD = args.backend_password
    elif has_config and configfile.has_option('Default', 'backend-password') and configfile.get('Default', 'backend-password'):
        BACKEND_PASSWORD = configfile.get('Default', 'backend-password')
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
    elif has_config and configfile.has_option('Default', 'counterparty-connect') and configfile.get('Default', 'counterparty-connect'):
        COUNTERPARTY_CONNECT = configfile.get('Default', 'counterparty-connect')
    else:
        COUNTERPARTY_CONNECT = 'localhost'

    global COUNTERPARTY_PORT
    if args.counterparty_port:
        COUNTERPARTY_PORT = args.counterparty_port
    elif has_config and configfile.has_option('Default', 'counterparty-port') and configfile.get('Default', 'counterparty-port'):
        COUNTERPARTY_PORT = configfile.get('Default', 'counterparty-port')
    else:
        COUNTERPARTY_PORT = 14000 if TESTNET else 4000
    try:
        COUNTERPARTY_PORT = int(COUNTERPARTY_PORT)
        assert int(COUNTERPARTY_PORT) > 1 and int(COUNTERPARTY_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number counterparty-port configuration parameter")
    
    global COUNTERPARTY_USER
    if args.counterparty_user:
        COUNTERPARTY_USER = args.counterparty_user
    elif has_config and configfile.has_option('Default', 'counterparty-user') and configfile.get('Default', 'counterparty-user'):
        COUNTERPARTY_USER = configfile.get('Default', 'counterparty-user')
    else:
        COUNTERPARTY_USER = 'rpc'

    global COUNTERPARTY_PASSWORD
    if args.counterparty_password:
        COUNTERPARTY_PASSWORD = args.counterparty_password
    elif has_config and configfile.has_option('Default', 'counterparty-password') and configfile.get('Default', 'counterparty-password'):
        COUNTERPARTY_PASSWORD = configfile.get('Default', 'counterparty-password')
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
    elif has_config and configfile.has_option('Default', 'mongodb-connect') and configfile.get('Default', 'mongodb-connect'):
        MONGODB_CONNECT = configfile.get('Default', 'mongodb-connect')
    else:
        MONGODB_CONNECT = 'localhost'

    global MONGODB_PORT
    if args.mongodb_port:
        MONGODB_PORT = args.mongodb_port
    elif has_config and configfile.has_option('Default', 'mongodb-port') and configfile.get('Default', 'mongodb-port'):
        MONGODB_PORT = configfile.get('Default', 'mongodb-port')
    else:
        MONGODB_PORT = 27017
    try:
        MONGODB_PORT = int(MONGODB_PORT)
        assert int(MONGODB_PORT) > 1 and int(MONGODB_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number mongodb-port configuration parameter")
            
    global MONGODB_DATABASE
    if args.mongodb_database:
        MONGODB_DATABASE = args.mongodb_database
    elif has_config and configfile.has_option('Default', 'mongodb-database') and configfile.get('Default', 'mongodb-database'):
        MONGODB_DATABASE = configfile.get('Default', 'mongodb-database')
    else:
        MONGODB_DATABASE = 'counterblockd_testnet' if TESTNET else 'counterblockd'

    global MONGODB_USER
    if args.mongodb_user:
        MONGODB_USER = args.mongodb_user
    elif has_config and configfile.has_option('Default', 'mongodb-user') and configfile.get('Default', 'mongodb-user'):
        MONGODB_USER = configfile.get('Default', 'mongodb-user')
    else:
        MONGODB_USER = None

    global MONGODB_PASSWORD
    if args.mongodb_password:
        MONGODB_PASSWORD = args.mongodb_password
    elif has_config and configfile.has_option('Default', 'mongodb-password') and configfile.get('Default', 'mongodb-password'):
        MONGODB_PASSWORD = configfile.get('Default', 'mongodb-password')
    else:
        MONGODB_PASSWORD = None

    # redis-related
    global REDIS_CONNECT
    if args.redis_connect:
        REDIS_CONNECT = args.redis_connect
    elif has_config and configfile.has_option('Default', 'redis-connect') and configfile.get('Default', 'redis-connect'):
        REDIS_CONNECT = configfile.get('Default', 'redis-connect')
    else:
        REDIS_CONNECT = '127.0.0.1'

    global REDIS_PORT
    if args.redis_port:
        REDIS_PORT = args.redis_port
    elif has_config and configfile.has_option('Default', 'redis-port') and configfile.get('Default', 'redis-port'):
        REDIS_PORT = configfile.get('Default', 'redis-port')
    else:
        REDIS_PORT = 6379
    try:
        REDIS_PORT = int(REDIS_PORT)
        assert int(REDIS_PORT) > 1 and int(REDIS_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number redis-port configuration parameter")

    global REDIS_DATABASE
    if args.redis_database:
        REDIS_DATABASE = args.redis_database
    elif has_config and configfile.has_option('Default', 'redis-database') and configfile.get('Default', 'redis-database'):
        REDIS_DATABASE = configfile.get('Default', 'redis-database')
    else:
        REDIS_DATABASE = 1 if TESTNET else 0
    try:
        REDIS_DATABASE = int(REDIS_DATABASE)
        assert int(REDIS_DATABASE) >= 0 and int(REDIS_DATABASE) <= 16
    except:
        raise Exception("Please specific a valid redis-database configuration parameter (between 0 and 16 inclusive)")

    global REDIS_ENABLE_APICACHE
    if args.redis_enable_apicache:
        REDIS_ENABLE_APICACHE = args.redis_enable_apicache
    elif has_config and configfile.has_option('Default', 'redis-enable-apicache') and configfile.get('Default', 'redis-enable-apicache'):
        REDIS_ENABLE_APICACHE = configfile.getboolean('Default', 'redis-enable-apicache')
    else:
        REDIS_ENABLE_APICACHE = False

    ##############
    # THINGS WE SERVE
    
    global RPC_HOST
    if args.rpc_host:
        RPC_HOST = args.rpc_host
    elif has_config and configfile.has_option('Default', 'rpc-host') and configfile.get('Default', 'rpc-host'):
        RPC_HOST = configfile.get('Default', 'rpc-host')
    else:
        RPC_HOST = 'localhost'

    global RPC_PORT
    if args.rpc_port:
        RPC_PORT = args.rpc_port
    elif has_config and configfile.has_option('Default', 'rpc-port') and configfile.get('Default', 'rpc-port'):
        RPC_PORT = configfile.get('Default', 'rpc-port')
    else:
        RPC_PORT = 14100 if TESTNET else 4100
    try:
        RPC_PORT = int(RPC_PORT)
        assert int(RPC_PORT) > 1 and int(RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number rpc-port configuration parameter")

    global RPC_ALLOW_CORS
    if args.rpc_allow_cors:
        RPC_ALLOW_CORS = args.rpc_allow_cors
    elif has_config and configfile.has_option('Default', 'rpc-allow-cors'):
        RPC_ALLOW_CORS = configfile.getboolean('Default', 'rpc-allow-cors')
    else:
        RPC_ALLOW_CORS = True
        
    #Other things
    global SUBDIR_ASSET_IMAGES
    SUBDIR_ASSET_IMAGES = "asset_img%s" % net_path_part #goes under the data dir and stores retrieved asset images
    global SUBDIR_FEED_IMAGES
    SUBDIR_FEED_IMAGES = "feed_img%s" % net_path_part #goes under the data dir and stores retrieved feed images

    ##############
    # OTHER SETTINGS

    # System (logging, pids, etc)
    global COUNTERBLOCKD_DIR
    COUNTERBLOCKD_DIR = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
    
    global LOG
    if args.log_file:
        LOG = args.log_file
    elif has_config and configfile.has_option('Default', 'log-file'):
        LOG = configfile.get('Default', 'log-file')
    else:
        LOG = os.path.join(log_dir, 'server%s.log' % net_path_part)

    global LOG_SIZE_KB
    if args.log_size_kb:
        LOG_SIZE_KB = args.log_size_kb
    elif has_config and configfile.has_option('Default', 'log-size-kb') and configfile.get('Default', 'log-size-kb'):
        LOG_SIZE_KB = configfile.get('Default', 'log-size-kb')
    else:
        LOG_SIZE_KB = 20000
    try:
        LOG_SIZE_KB = int(LOG_SIZE_KB)
        assert LOG_SIZE_KB > 0
    except:
        raise Exception("Please specific a valid log-size-kb value (in kilobytes)")

    global LOG_NUM_FILES
    if args.log_num_files:
        LOG_NUM_FILES = args.log_num_files
    elif has_config and configfile.has_option('Default', 'log-num-files') and configfile.get('Default', 'log-num-files'):
        LOG_NUM_FILES = configfile.get('Default', 'log-num-files')
    else:
        LOG_NUM_FILES = 5
    try:
        LOG_NUM_FILES = int(LOG_NUM_FILES)
        assert LOG_NUM_FILES > 0 and LOG_NUM_FILES <= 100
    except:
        raise Exception("Please specific a valid log-num-files value (must be less than 100)")
                
    global TX_LOG
    if args.tx_log_file:
        TX_LOG = args.tx_log_file
    elif has_config and configfile.has_option('Default', 'tx-log-file'):
        TX_LOG = configfile.get('Default', 'tx-log-file')
    else:
        TX_LOG = os.path.join(log_dir, 'server%s.tx.log' % net_path_part)

    global PID
    if args.pid_file:
        PID = args.pid_file
    elif has_config and configfile.has_option('Default', 'pid-file'):
        PID = configfile.get('Default', 'pid-file')
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
    init_data_dir(args)
    load(args)
    load_schemas()
