# -*- coding: utf-8 -*-

##
## CONSTANTS
##
VERSION = "1.0.0" #should keep up with counterblockd repo's release tag

DB_VERSION = 22 #a db version increment will cause counterblockd to rebuild its database off of counterpartyd 

UNIT = 100000000

SUBDIR_ASSET_IMAGES = "asset_img" #goes under the data dir and stores retrieved asset images
SUBDIR_FEED_IMAGES = "feed_img" #goes under the data dir and stores retrieved feed images

MARKET_PRICE_DERIVE_NUM_POINTS = 8 #number of last trades over which to derive the market price (via WVAP)

# FROM counterpartyd
# NOTE: These constants must match those in counterpartyd/lib/py
REGULAR_DUST_SIZE = 5430
MULTISIG_DUST_SIZE = 5430 * 2
ORDER_BTC_DUST_LIMIT_CUTOFF = MULTISIG_DUST_SIZE

BTC = 'BTC'
XCP = 'XCP'

MAX_REORG_NUM_BLOCKS = 10 #max reorg we'd likely ever see
MAX_FORCED_REORG_NUM_BLOCKS = 20 #but let us go deeper when messages are out of sync

ARMORY_UTXSVR_PORT_MAINNET = 6590
ARMORY_UTXSVR_PORT_TESTNET = 6591

QUOTE_ASSETS = ['BTC', 'XBTC', 'XCP'] # define the priority for quote asset
MARKET_LIST_QUOTE_ASSETS = ['XCP', 'XBTC', 'BTC'] # define the order in the market list

DEFAULT_BACKEND_RPC_PORT_TESTNET = 18332
DEFAULT_BACKEND_RPC_PORT = 8332


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

    global DATA_DIR
    if not args.data_dir:
        DATA_DIR = appdirs.user_data_dir(appauthor='Counterparty', appname='counterblockd', roaming=True)
    else:
        DATA_DIR = args.data_dir
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)

def load(args):
    import os
    import ConfigParser
    import email.utils
    
    assert DATA_DIR
    
    #Read config file
    configfile = ConfigParser.ConfigParser()
    config_path = os.path.join(DATA_DIR, 'counterblockd.conf')
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
    global BACKEND_RPC_CONNECT
    if args.counterpartyd_rpc_connect:
        BACKEND_RPC_CONNECT = args.backend_rpc_connect
    elif has_config and configfile.has_option('Default', 'backend-rpc-connect') and configfile.get('Default', 'backend-rpc-connect'):
        BACKEND_RPC_CONNECT = configfile.get('Default', 'backend-rpc-connect')
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-connect') and configfile.get('Default', 'bitcoind-rpc-connect'):
        BACKEND_RPC_CONNECT = configfile.get('Default', 'bitcoind-rpc-connect')
    else:
        BACKEND_RPC_CONNECT = 'localhost'

    global BACKEND_RPC_PORT
    if args.backend_rpc_port:
        BACKEND_RPC_PORT = args.backend_rpc_port
    elif has_config and configfile.has_option('Default', 'backend-rpc-port') and configfile.get('Default', 'backend-rpc-port'):
        BACKEND_RPC_PORT = configfile.get('Default', 'backend-rpc-port')
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-port') and configfile.get('Default', 'bitcoind-rpc-port'):
        BACKEND_RPC_PORT = configfile.get('Default', 'bitcoind-rpc-port')
    else:
        if TESTNET:
            BACKEND_RPC_PORT = DEFAULT_BACKEND_RPC_PORT_TESTNET
        else:
            BACKEND_RPC_PORT = DEFAULT_BACKEND_RPC_PORT
    try:
        BACKEND_RPC_PORT = int(BACKEND_RPC_PORT)
        assert int(BACKEND_RPC_PORT) > 1 and int(BACKEND_RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number backend-rpc-port configuration parameter")
            
    global BACKEND_RPC_USER
    if args.backend_rpc_user:
        BACKEND_RPC_USER = args.backend_rpc_user
    elif has_config and configfile.has_option('Default', 'backend-rpc-user') and configfile.get('Default', 'backend-rpc-user'):
        BACKEND_RPC_USER = configfile.get('Default', 'backend-rpc-user')
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-user') and configfile.get('Default', 'bitcoind-rpc-user'):
        BACKEND_RPC_USER = configfile.get('Default', 'bitcoind-rpc-user')
    else:
        BACKEND_RPC_USER = 'rpcuser'

    global BACKEND_RPC_PASSWORD
    if args.backend_rpc_password:
        BACKEND_RPC_PASSWORD = args.backend_rpc_password
    elif has_config and configfile.has_option('Default', 'backend-rpc-password') and configfile.get('Default', 'backend-rpc-password'):
        BACKEND_RPC_PASSWORD = configfile.get('Default', 'backend-rpc-password')
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-password') and configfile.get('Default', 'bitcoind-rpc-password'):
        BACKEND_RPC_PASSWORD = configfile.get('Default', 'bitcoind-rpc-password')
    else:
        BACKEND_RPC_PASSWORD = 'rpcpassword'

    global BACKEND_RPC
    BACKEND_RPC = 'http://' + BACKEND_RPC_CONNECT + ':' + str(BACKEND_RPC_PORT) + '/'

    global BACKEND_AUTH
    BACKEND_AUTH = (BACKEND_RPC_USER, BACKEND_RPC_PASSWORD) if (BACKEND_RPC_USER and BACKEND_RPC_PASSWORD) else None
    
    global BACKEND_RPC_URL
    BACKEND_RPC_URL = 'http://' + BACKEND_RPC_USER + ':' + BACKEND_RPC_PASSWORD + '@' + BACKEND_RPC_CONNECT + ':' + str(BACKEND_RPC_PORT)

    # counterpartyd RPC connection
    global COUNTERPARTYD_RPC_CONNECT
    if args.counterpartyd_rpc_connect:
        COUNTERPARTYD_RPC_CONNECT = args.counterpartyd_rpc_connect
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-connect') and configfile.get('Default', 'counterpartyd-rpc-connect'):
        COUNTERPARTYD_RPC_CONNECT = configfile.get('Default', 'counterpartyd-rpc-connect')
    else:
        COUNTERPARTYD_RPC_CONNECT = 'localhost'

    global COUNTERPARTYD_RPC_PORT
    if args.counterpartyd_rpc_port:
        COUNTERPARTYD_RPC_PORT = args.counterpartyd_rpc_port
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-port') and configfile.get('Default', 'counterpartyd-rpc-port'):
        COUNTERPARTYD_RPC_PORT = configfile.get('Default', 'counterpartyd-rpc-port')
    else:
        if TESTNET:
            COUNTERPARTYD_RPC_PORT = 14000
        else:
            COUNTERPARTYD_RPC_PORT = 4000
    try:
        COUNTERPARTYD_RPC_PORT = int(COUNTERPARTYD_RPC_PORT)
        assert int(COUNTERPARTYD_RPC_PORT) > 1 and int(COUNTERPARTYD_RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number counterpartyd-rpc-port configuration parameter")
    
    global COUNTERPARTYD_RPC_USER
    if args.counterpartyd_rpc_user:
        COUNTERPARTYD_RPC_USER = args.counterpartyd_rpc_user
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-user') and configfile.get('Default', 'counterpartyd-rpc-user'):
        COUNTERPARTYD_RPC_USER = configfile.get('Default', 'counterpartyd-rpc-user')
    else:
        COUNTERPARTYD_RPC_USER = 'rpcuser'

    global COUNTERPARTYD_RPC_PASSWORD
    if args.counterpartyd_rpc_password:
        COUNTERPARTYD_RPC_PASSWORD = args.counterpartyd_rpc_password
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-password') and configfile.get('Default', 'counterpartyd-rpc-password'):
        COUNTERPARTYD_RPC_PASSWORD = configfile.get('Default', 'counterpartyd-rpc-password')
    else:
        COUNTERPARTYD_RPC_PASSWORD = 'rpcpassword'

    global COUNTERPARTYD_RPC
    COUNTERPARTYD_RPC = 'http://' + COUNTERPARTYD_RPC_CONNECT + ':' + str(COUNTERPARTYD_RPC_PORT) + '/api/'
    
    global COUNTERPARTYD_AUTH
    COUNTERPARTYD_AUTH = (COUNTERPARTYD_RPC_USER, COUNTERPARTYD_RPC_PASSWORD) if (COUNTERPARTYD_RPC_USER and COUNTERPARTYD_RPC_PASSWORD) else None

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
        if TESTNET:
            MONGODB_DATABASE = 'counterblockd_testnet'
        else:
            MONGODB_DATABASE = 'counterblockd'

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
        if TESTNET:
            REDIS_DATABASE = 1
        else:
            REDIS_DATABASE = 0
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
        if TESTNET:
            RPC_PORT = 14100
        else:
            RPC_PORT = 4100        
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

    global SOCKETIO_HOST
    if args.socketio_host:
        SOCKETIO_HOST = args.socketio_host
    elif has_config and configfile.has_option('Default', 'socketio-host') and configfile.get('Default', 'socketio-host'):
        SOCKETIO_HOST = configfile.get('Default', 'socketio-host')
    else:
        SOCKETIO_HOST = 'localhost'

    global SOCKETIO_PORT
    if args.socketio_port:
        SOCKETIO_PORT = args.socketio_port
    elif has_config and configfile.has_option('Default', 'socketio-port') and configfile.get('Default', 'socketio-port'):
        SOCKETIO_PORT = configfile.get('Default', 'socketio-port')
    else:
        if TESTNET:
            SOCKETIO_PORT = 14101
        else:
            SOCKETIO_PORT = 4101        
    try:
        SOCKETIO_PORT = int(SOCKETIO_PORT)
        assert int(SOCKETIO_PORT) > 1 and int(SOCKETIO_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number socketio-port configuration parameter")
    
    global SOCKETIO_CHAT_HOST
    if args.socketio_chat_host:
        SOCKETIO_CHAT_HOST = args.socketio_chat_host
    elif has_config and configfile.has_option('Default', 'socketio-chat-host') and configfile.get('Default', 'socketio-chat-host'):
        SOCKETIO_CHAT_HOST = configfile.get('Default', 'socketio-chat-host')
    else:
        SOCKETIO_CHAT_HOST = 'localhost'

    global SOCKETIO_CHAT_PORT
    if args.socketio_chat_port:
        SOCKETIO_CHAT_PORT = args.socketio_chat_port
    elif has_config and configfile.has_option('Default', 'socketio-chat-port') and configfile.get('Default', 'socketio-chat-port'):
        SOCKETIO_CHAT_PORT = configfile.get('Default', 'socketio-chat-port')
    else:
        if TESTNET:
            SOCKETIO_CHAT_PORT = 14102
        else:
            SOCKETIO_CHAT_PORT = 4102       
    try:
        SOCKETIO_CHAT_PORT = int(SOCKETIO_CHAT_PORT)
        assert int(SOCKETIO_CHAT_PORT) > 1 and int(SOCKETIO_CHAT_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number socketio-chat-port configuration parameter")


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
        LOG = os.path.join(DATA_DIR, 'counterblockd.log')
    
    global TX_LOG
    if args.tx_log_file:
        TX_LOG = args.tx_log_file
    elif has_config and configfile.has_option('Default', 'tx-log-file'):
        TX_LOG = configfile.get('Default', 'tx-log-file')
    else:
        TX_LOG = os.path.join(DATA_DIR, 'counterblockd-tx.log')

    global PID
    if args.pid_file:
        PID = args.pid_file
    elif has_config and configfile.has_option('Default', 'pid-file'):
        PID = configfile.get('Default', 'pid-file')
    else:
        PID = os.path.join(DATA_DIR, 'counterblockd.pid')

    #email-related
    global SUPPORT_EMAIL
    if args.support_email:
        SUPPORT_EMAIL = args.support_email
    elif has_config and configfile.has_option('Default', 'support-email') and configfile.get('Default', 'support-email'):
        SUPPORT_EMAIL = configfile.get('Default', 'support-email')
    else:
        SUPPORT_EMAIL = None #disable support tickets
    if SUPPORT_EMAIL:
        if not email.utils.parseaddr(SUPPORT_EMAIL)[1]:
            raise Exception("Invalid support email address")

    global EMAIL_SERVER
    if args.email_server:
        EMAIL_SERVER = args.email_server
    elif has_config and configfile.has_option('Default', 'email-server') and configfile.get('Default', 'email-server'):
        EMAIL_SERVER = configfile.get('Default', 'email-server')
    else:
        EMAIL_SERVER = "localhost"

    ###
    # TODO: MOVE OUT INTO THEIR OWN PLUGINS
    # armory integration
    global ARMORY_UTXSVR_ENABLE
    if args.armory_utxsvr_enable:
        ARMORY_UTXSVR_ENABLE = args.armory_utxsvr_enable
    elif has_config and configfile.has_option('Default', 'armory-utxsvr-enable') and configfile.getboolean('Default', 'armory-utxsvr-enable'):
        ARMORY_UTXSVR_ENABLE = configfile.get('Default', 'armory-utxsvr-enable')
    else:
        ARMORY_UTXSVR_ENABLE = False

    #vending machine integration
    global VENDING_MACHINE_PROVIDER
    if args.vending_machine_provider:
        VENDING_MACHINE_PROVIDER = args.vending_machine_provider
    elif has_config and configfile.has_option('Default', 'vending-machine-provider') and configfile.get('Default', 'vending-machine-provider'):
        VENDING_MACHINE_PROVIDER = configfile.get('Default', 'vending-machine-provider')
    else:
        VENDING_MACHINE_PROVIDER = None

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
