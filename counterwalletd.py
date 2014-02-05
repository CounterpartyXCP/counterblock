#! /usr/bin/env python3
"""
counterwalletd server

Credits for a lot of the zeromq/socket.io plumbing goes to:
   http://sourceforge.net/u/rick446/gevent-socketio/ci/master/tree/demo.py
"""

#import before importing other modules
import gevent
from gevent import monkey; monkey.patch_all()

import os
import argparse
import json
import copy
import logging
import datetime
import appdirs
import ConfigParser
import time

import pymongo
import cube
import boto.dynamodb

import redis
import redis.connection
redis.connection.socket = gevent.socket #make redis play well with gevent

from socketio import server as socketio_server
from requests.auth import HTTPBasicAuth

from lib import (config, api, siofeeds, util)

CUBE_ENTITIES = ['credit', 'debit', 'burn', 'cancel', 'issuance', 'order', 'send']

def poll_counterpartyd(mongo_db, mongo_cube_db, cube_client, to_socketio_queue):
    def prune_my_stale_blocks(max_block_index, max_block_time):
        """called if there are any records for blocks higher than this in the database? If so, they were impartially created
           and we should get rid of them"""
        for entity in CUBE_ENTITIES:
            #delete the cube events
            mongo_db[entity+'_events'].remove({"block_index": {"$gt": max_block_index}})
            #invaliate the cube metrics by setting metric.i to True for any metrics with a time >= last block time
            mongo_db[entity+'_metrics'].update({"_id.t": {"$gte": max_block_time}}, {"$set": {"i": True}})
            #^ TODO: not 100% sure this will work, needs more testing :)

    #get the last processed block out of mongo
    my_latest_block = mongo_db.processed_blocks.find_one(sort=[("block_index", -1)])
    if not my_latest_block:
        my_latest_block = {'block_index': 0, 'block_time': datetime.datetime.utcfromtimestamp(0), 'block_hash': None}
    
    #remove any data we have for blocks higher than this (would happen if counterwalletd, cube, or mongo died
    # or erroed out while processing a block)
    prune_my_stale_blocks(my_latest_block['block_index'], my_latest_block['block_time'])
    
    #start polling counterpartyd for new blocks    
    while True:
        try:
            result = util.call_jsonrpc_api("get_last_processed_block", abort_on_error=True)
            #^ REMEMBER: last_processed_block['block_time'] is an int, not a datetime object
        except Exception, e:
            logging.warn(str(e) + " Waiting 5 seconds before trying again...")
            time.sleep(5)
            continue
        
        #new block - get the various different entities that could exist under that block
        if my_latest_block['block_index'] < last_processed_block['block_index']:
            #need to catch up
            try:
                block_data = util.call_jsonrpc_api("get_data_for_block",
                    [my_latest_block['block_index'] + 1], abort_on_error=True)
            except Exception, e:
                logging.warn(str(e) + " Waiting 5 seconds before trying again...")
                time.sleep(5)
                continue
            
            #add relevant data into cube
            block_time_str = my_latest_block['block_time'].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            for entity in CUBE_ENTITIES:
                raise NotImplementedError("TODO: determine how the data from get_data_for_block comes back")
                #assert entity in block_data #should be a list (even if empty)

                #cube_client.put(entity, data)
                
                #data = WHATEVER
                
                #for each new entity, send out a message via socket.io
                event = {
                    'event': entity,
                    'block_time': time.mktime(my_latest_block['block_time'].timetuple()),
                    'block_time_str': block_time_str,
                    'msg': data
                }
                to_socketio_queue.put(event)
            
            #block successfully processed, add into our DB
            new_block = {
                'block_index': block_data['block_index'],
                'block_time': datetime.datetime.fromutctimestamp(block_data['block_time']),
                'block_hash': block_data['block_hash'],
            }
            mongo_db.processed_blocks.insert(new_block)
            
        elif my_latest_block['block_index'] > last_processed_block['block_index']:
            #we have stale blocks (i.e. a reorg happened in counterpartyd)
            prune_my_stale_blocks(last_processed_block['block_index'], last_processed_block['block_time'])
            
        time.sleep(2) #wait a bit to query again for the latest block


if __name__ == '__main__':
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(prog='counterwalletd', description='Counterwallet daemon. Works with counterpartyd')
    parser.add_argument('-V', '--version', action='version', version="counterwalletd v%s" % config.VERSION)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='sets log level to DEBUG instead of WARNING')

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

    parser.add_argument('--dynamodb-enable', action='store_true', default=True, help='set to false to use MongoDB instead of DynamoDB for wallet preferences and chat handle storage')
    parser.add_argument('--dynamodb-aws-region', help='the Amazon Web Services region to use for DynamoDB connection (preferences storage)')
    parser.add_argument('--dynamodb-aws-key', help='the Amazon Web Services key to use for DynamoDB connection (preferences storage)')
    parser.add_argument('--dynamodb-aws-secret', type=int, help='the Amazon Web Services secret to use for DynamoDB connection (preferences storage)')

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
        config.BITCOIND_RPC_PORT = 8332
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


    # dynamodb-enable
    if args.dynamodb_enable:
        config.DYNAMODB_ENABLE = args.dynamodb_enable
    elif has_config and configfile.has_option('Default', 'dynamodb-enable') and configfile.get('Default', 'dynamodb-enable'):
        config.DYNAMODB_ENABLE = configfile.getboolean('Default', 'dynamodb-enable')
    else:
        config.DYNAMODB_ENABLE = True
    
    #dynamodb-aws-region
    if args.dynamodb_aws_region:
        config.DYNAMODB_AWS_REGION = args.dynamodb_aws_region
    elif has_config and configfile.has_option('Default', 'dynamodb-aws-region') and configfile.get('Default', 'dynamodb-aws-region'):
        config.DYNAMODB_AWS_REGION = configfile.get('Default', 'dynamodb-aws-region')
    else:
        config.DYNAMODB_AWS_REGION = 'eu-west-1' #Ireland

    #dynamodb-aws-key
    if args.dynamodb_aws_key:
        config.DYNAMODB_AWS_KEY = args.dynamodb_aws_key
    elif has_config and configfile.has_option('Default', 'dynamodb-aws-key') and configfile.get('Default', 'dynamodb-aws-key'):
        config.DYNAMODB_AWS_KEY = configfile.get('Default', 'dynamodb-aws-key')
    else:
        config.DYNAMODB_AWS_KEY = None

    #dynamodb-aws-secret
    if args.dynamodb_aws_secret:
        config.DYNAMODB_AWS_SECRET = args.dynamodb_aws_secret
    elif has_config and configfile.has_option('Default', 'dynamodb-aws-secret') and configfile.get('Default', 'dynamodb-aws-secret'):
        config.DYNAMODB_AWS_SECRET = configfile.get('Default', 'dynamodb-aws-secret')
    else:
        config.DYNAMODB_AWS_SECRET = None
    
    if config.DYNAMODB_ENABLE and (not config.DYNAMODB_AWS_KEY or not config.DYNAMODB_AWS_SECRET):
        raise Exception("If 'dynamodb-enable' is set to True, you must specify values for both 'dynamodb-aws-key' and 'dynamodb-aws-secret'")

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

    # cube host
    if args.cube_connect:
        config.CUBE_CONNECT = args.cube_connect
    elif has_config and configfile.has_option('Default', 'cube-connect') and configfile.get('Default', 'cube-connect'):
        config.CUBE_CONNECT = configfile.get('Default', 'cube-connect')
    else:
        config.CUBE_CONNECT = 'localhost'

    # cube collector port
    if args.cube_collector_port:
        config.CUBE_COLLECTOR_PORT = args.cube_collector_port
    elif has_config and configfile.has_option('Default', 'cube-collector-port') and configfile.get('Default', 'cube-collector-port'):
        config.CUBE_COLLECTOR_PORT = configfile.get('Default', 'cube-collector-port')
    else:
        config.CUBE_COLLECTOR_PORT = 1080
    try:
        int(config.CUBE_COLLECTOR_PORT)
        assert int(config.CUBE_COLLECTOR_PORT) > 1 and int(config.CUBE_COLLECTOR_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number cube-collector-port configuration parameter")

    # cube evaluator port
    if args.cube_evaluator_port:
        config.CUBE_EVALUATOR_PORT = args.cube_evaluator_port
    elif has_config and configfile.has_option('Default', 'cube-evaluator-port') and configfile.get('Default', 'cube-evaluator-port'):
        config.CUBE_EVALUATOR_PORT = configfile.get('Default', 'cube-evaluator-port')
    else:
        config.CUBE_EVALUATOR_PORT = 1081
    try:
        int(config.CUBE_EVALUATOR_PORT)
        assert int(config.CUBE_EVALUATOR_PORT) > 1 and int(config.CUBE_EVALUATOR_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number cube-evaluator-port configuration parameter")

    # cube database
    if args.cube_database:
        config.CUBE_DATABASE = args.cube_database
    elif has_config and configfile.has_option('Default', 'cube-database') and configfile.get('Default', 'cube-database'):
        config.CUBE_DATABASE = configfile.get('Default', 'cube-database')
    else:
        config.CUBE_DATABASE = 'cube_development'


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
        config.SOCKETIO_CHAT_PORT = 4102
    try:
        int(config.SOCKETIO_CHAT_PORT)
        assert int(config.SOCKETIO_CHAT_PORT) > 1 and int(config.SOCKETIO_CHAT_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number socketio-chat-port configuration parameter")

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
    formatter = logging.Formatter('%(message)s')
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
    mongo_db.processed_blocks.ensure_index('block_index')
    for entity in CUBE_ENTITIES: #ensure indexing for cube data
        mongo_db[entity+'_events'].ensure_index([("block_index", pymongo.DESCENDING), ("t", pymongo.ASCENDING)])
    if not config.DYNAMODB_ENABLE: #if using mongo for prefs and chat handle storage
        mongo_db.preferences.ensure_index('wallet_id', unique=True)
        mongo_db.chat_handles.ensure_index('wallet_id', unique=True)
        mongo_db.chat_handles.ensure_index('handle', unique=True)
    
    #Connect to cube
    cube_client = cube.Cube(hostname=config.CUBE_CONNECT,
        collector_port=config.CUBE_COLLECTOR_PORT, evaluator_port=config.CUBE_EVALUATOR_PORT)
    mongo_cube_db = mongo_client[config.CUBE_DATABASE] 
    
    #Connect to redis
    if config.REDIS_ENABLE_APICACHE:
        logging.info("Enabling redis read API caching... (%s:%s)" % (config.REDIS_CONNECT, config.REDIS_PORT))
        redis_client = redis.StrictRedis(host=config.REDIS_CONNECT, port=config.REDIS_PORT, db=config.REDIS_DATABASE)
    else:
        redis_client = None
    
    #Optionally connect to dynamodb (for wallet prefs storage)
    if config.DYNAMODB_ENABLE:
        from boto import dynamodb2
        from boto.dynamodb2.fields import HashKey, GlobalAllIndex
        from boto.dynamodb2.table import Table
        from boto.dynamodb2.types import STRING
        #import boto
        #boto.set_stream_logger('boto')
        
        #boto seems to have a bug where it doesn't like access creds passed into the constructor, so throw
        # them in the environment instead
        os.environ['AWS_ACCESS_KEY_ID'] = config.DYNAMODB_AWS_KEY
        os.environ['AWS_SECRET_ACCESS_KEY'] = config.DYNAMODB_AWS_SECRET

        dynamodb_client = dynamodb2.connect_to_region(config.DYNAMODB_AWS_REGION)
        tables = dynamodb_client.list_tables()['TableNames']
        
        #make sure preferences domain exists
        if 'preferences' not in tables:
            logging.info("Creating 'preferences' domain in DynamoDB as it doesn't exist...")
            dynamo_preferences_table = Table.create('preferences', schema=[
                HashKey('wallet_id', data_type=STRING)
            ], throughput={
                'read':20,
                'write': 10,
            }, connection=dynamodb_client)
        else:
            dynamo_preferences_table = Table('preferences',
                schema=[HashKey('wallet_id', data_type=STRING)],
                connection=dynamodb_client
            )

        if 'chat_handles' not in tables:
            dynamo_chat_handles_table = Table.create('chat_handles', schema=[
                HashKey('wallet_id', data_type=STRING)
            ], throughput={
                'read':16,
                'write': 8,
            }, global_indexes=[
                GlobalAllIndex('WalletsByHandle', parts=[ HashKey('handle', data_type=STRING) ],
                throughput={
                  'read':4,
                  'write':2,
                }),
            ], connection=dynamodb_client)
        else:
            dynamo_chat_handles_table = Table('chat_handles',
                schema=[HashKey('wallet_id', data_type=STRING)],
                indexes=[GlobalAllIndex('WalletsByHandle', parts=[ HashKey('handle', data_type=STRING) ])],
                connection=dynamodb_client
            )
    else:
        dynamodb_client = None
        dynamo_preferences_table = None
        dynamo_chat_handles_table = None
    
    to_socketio_queue = gevent.queue.Queue()
    
    logging.info("Starting up socket.io server (counterpartyd event feed)...")
    sio_server = socketio_server.SocketIOServer(
        (config.SOCKETIO_HOST, config.SOCKETIO_PORT),
        siofeeds.SocketIOEventServer(to_socketio_queue),
        resource="socket.io", policy_server=False)
    sio_server.start() #start the socket.io server greenlets

    logging.info("Starting up socket.io server (counterwallet chat)...")
    sio_server = socketio_server.SocketIOServer(
        (config.SOCKETIO_CHAT_HOST, config.SOCKETIO_CHAT_PORT),
        siofeeds.SocketIOChatServer(mongo_db, dynamo_chat_handles_table),
        resource="socket.io", policy_server=False)
    sio_server.start() #start the socket.io server greenlets

    logging.info("DISABLED FOR NOW: Starting up counterpartyd block poller...")
    #gevent.spawn(poll_counterpartyd, mongo_db, mongo_cube_db, cube_client, to_socketio_queue)

    logging.info("Starting up RPC API handler...")
    api.serve_api(mongo_db, dynamo_preferences_table, dynamo_chat_handles_table, redis_client)

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
