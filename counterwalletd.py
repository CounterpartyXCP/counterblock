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
import redis
import redis.connection
redis.connection.socket = gevent.socket #make redis play well with gevent

from socketio import server as socketio_server
from requests.auth import HTTPBasicAuth

from lib import (config, api, siofeeds, util)

def poll_counterpartyd(mongo_db, to_socketio_queue):
    def clear_on_new_db_version(running_info, prefs, my_latest_block, force=False):
        #check for a new DB version
        wipeState = False
        if force:
            wipeState = True
        elif running_info['db_version_major'] != prefs['counterpartyd_db_version_major']:
            logging.warn("counterpartyd MAJOR DB version change (we built from %s, counterpartyd is at %s). Wiping our state data." % (
                prefs['counterpartyd_db_version_major'], running_info['db_version_major']))
            wipeState = True
        elif running_info['db_version_minor'] != prefs['counterpartyd_db_version_minor']:
            logging.warn("counterpartyd MINOR DB version change (we built from %s.%s, counterpartyd is at %s.%s). Wiping our state data." % (
                prefs['counterpartyd_db_version_major'], prefs['counterpartyd_db_version_minor'],
                running_info['db_version_major'], running_info['db_version_minor']))
            wipeState = True
        if wipeState:
            #boom! blow away all collections in mongo
            mongo_db.processed_blocks.remove()
            mongo_db.trades.remove()
            #update our DB prefs
            prefs['db_version'] = config.DB_VERSION
            prefs['counterpartyd_db_version_major'] = running_info['db_version_major'] 
            prefs['counterpartyd_db_version_minor'] = running_info['db_version_minor']
            mongo_db.prefs.update({}, prefs)
            
            #reset my latest block record
            my_latest_block = {'block_index': config.BLOCK_FIRST, 'block_time': None, 'block_hash': None}
            config.CAUGHT_UP = False #You've Come a Long Way, Baby
        return prefs, my_latest_block
        
    def prune_my_stale_blocks(max_block_index, max_block_time):
        """called if there are any records for blocks higher than this in the database? If so, they were impartially created
           and we should get rid of them"""
        mongo_db.processed_blocks.remove({"block_index": {"$gt": max_block_index}})
        mongo_db.trades.remove({"block_index": {"$gt": max_block_index}})

    #grab our stored preferences
    prefs_created = False
    prefs = mongo_db.prefs.find()
    assert prefs.count() in [0, 1]
    if prefs.count() == 0:
        mongo_db.prefs.insert({ #create default prefs object
            'db_version': config.DB_VERSION, #counterwalletd database version
            'counterpartyd_db_version_major': None,
            'counterpartyd_db_version_minor': None,
        })
        prefs = mongo_db.prefs.find()[0]
        prefs_created = True
    else:
        prefs = prefs[0]
        
    #get the last processed block out of mongo
    my_latest_block = mongo_db.processed_blocks.find_one(sort=[("block_index", -1)])
    if not my_latest_block:
        my_latest_block = {'block_index': config.BLOCK_FIRST, 'block_time': None, 'block_hash': None}

    #see if DB version has increased and rebuild if so
    if prefs_created or prefs['db_version'] != config.DB_VERSION:
        prefs, my_latest_block = clear_on_new_db_version(None, prefs, my_latest_block, force=True)
    
    #remove any data we have for blocks higher than this (would happen if counterwalletd or mongo died
    # or errored out while processing a block)
    prune_my_stale_blocks(my_latest_block['block_index'], my_latest_block['block_time'])
    
    #start polling counterpartyd for new blocks    
    while True:
        try:
            running_info = util.call_jsonrpc_api("get_running_info", abort_on_error=True)['result']
        except Exception, e:
            logging.warn(str(e) + " Waiting 3 seconds before trying again...")
            time.sleep(3)
            continue
        
        #wipe our state data if necessary, if counterpartyd has moved on to a new DB version
        prefs, my_latest_block = clear_on_new_db_version(running_info, prefs, my_latest_block)
        
        ##########################
        #REORG detection: examine the last N processed block hashes from counterpartyd, and prune back if necessary
        #TODO!!!!
        #running_info['last_block_hashes'] =
        ########################## 
        
        #work up to what block counterpartyd is at
        last_processed_block = running_info['last_block']
        
        if last_processed_block['block_index'] is None:
            logging.warn("counterpartyd has no last processed block (probably is reparsing). Waiting 3 seconds before trying again...")
            time.sleep(3)
            continue
        
        if my_latest_block['block_index'] < last_processed_block['block_index']:
            #need to catch up
            config.CAUGHT_UP = False
            
            cur_block_index = my_latest_block['block_index'] + 1
            #get the blocktime for the next block we have to process 
            try:
                cur_block = util.call_jsonrpc_api("get_block_info", [cur_block_index,], abort_on_error=True)['result']
            except Exception, e:
                logging.warn(str(e) + " Waiting 3 seconds before trying again...")
                time.sleep(3)
                continue
            cur_block['block_time_obj'] = datetime.datetime.fromtimestamp(cur_block['block_time'])
            cur_block['block_time_str'] = cur_block['block_time_obj'].isoformat()
            
            logging.info("Processing block %i ..." % (cur_block_index,))
            try:
                block_data = util.call_jsonrpc_api("get_messages", [cur_block_index,], abort_on_error=True)['result']
            except Exception, e:
                logging.warn(str(e) + " Waiting 5 seconds before trying again...")
                time.sleep(5)
                continue
            
            #parse out response (list of txns, ordered as they appeared in the block)
            for msg in block_data:
                msg_data = json.loads(msg['bindings'])
                
                #track assets
                if msg['category'] == 'issuances':
                    if msg_data['amount'] == 0: #lock asset
                        mongo_db.tracked_assets.update(
                            {'asset': msg_data['asset']}, {"$set": {'locked': True}}, upsert=False)
                    elif msg_data['transfer']: #transfer asset
                        mongo_db.tracked_assets.update(
                            {'asset': msg_data['asset']}, {"$set": {'owner': msg_data['issuer']}}, upsert=False)
                    else: #issue new asset or issue addition qty of an asset
                        tracked_asset = mongo_db.tracked_assets.find_one({'asset': msg_data['asset']})
                        if not tracked_asset:
                            tracked_asset = {
                                'asset': msg_data['asset'],
                                'owner': msg_data['issuer'],
                                'divisible': msg_data['divisible'],
                                'locked': False,
                                'total_issued': msg_data['amount']
                            }
                            mongo_db.tracked_assets.insert(tracked_asset)
                        else:
                            mongo_db.tracked_assets.update(
                                {'asset': msg_data['asset']}, {'$inc': {'total_issued': msg_data['amount']}}, upsert=False)
                
                #track balance changes for each address
                if msg['category'] in ['credits', 'debits']:
                    amount = msg_data['amount'] if msg['category'] == 'credits' else -msg_data['amount']                    

                    #look up the previous balance to go off of
                    last_bal_change = mongo_db.balance_changes.find({
                        'address': msg_data['address'],
                        'asset': msg_data['asset']
                    }).sort("block_time", pymongo.DESCENDING).limit(1)
                    
                    if last_bal_change['block_index'] == cur_block_index:
                        #modify this record, as we want at most one entry per block index for each (address, asset) pair
                        last_bal_change['amount'] += amount 
                        last_bal_change['new_balance'] += amount
                        last_bal_change.save()
                        logging.info("Procesed BalChange (UPDATED) from tx %s :: %s" % (msg['message_index'], bal_change))
                    else:
                        bal_change = {
                            'address': msg_data['address'], 
                            'asset': msg_data['asset'],
                            'block_index': cur_block_index,
                            'block_time': cur_block['block_time'],
                            'amount': amount,
                            'new_balance': last_bal_change['new_balance'] + amount if last_bal_change else amount
                        }
                        mongo_db.balance_changes.insert(bal_change)
                        logging.info("Procesed BalChange from tx %s :: %s" % (msg['message_index'], bal_change))
                
                #book trades
                if     msg['category'] == 'btcpays' \
                   or (    msg['category'] == 'order_matches' \
                       and msg_data['forward_asset'] != 'BTC' \
                       and msg_data['backward_asset'] != 'BTC'):
                    #check if the referrenced assets are divisible
                    forward_asset_info = util.call_jsonrpc_api("get_asset_info", [msg_data['forward_asset'],], abort_on_error=True)['result']
                    backward_asset_info = util.call_jsonrpc_api("get_asset_info", [msg_data['backward_asset'],], abort_on_error=True)['result']
                    
                    if msg['category'] == 'btcpays':
                        tx0_hash, tx1_hash = msg_data['order_match_id'][:64], msg_data['order_match_id'][64:] 
                        #get the order_match this btcpay settles
                        order_match = util.call_jsonrpc_api("get_order_matches",
                            [{'field': 'tx0_hash', 'op': '==', 'value': tx0_hash},
                             {'field': 'tx1_hash', 'op': '==', 'value': tx1_hash}], abort_on_error=True)['result']
                    else:
                        order_match = msg_data
                    
                    base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
                    
                    #take divisible trade amounts to floating point
                    forward_amount = float(msg_data['forward_amount']) / config.UNIT if forward_asset_info['divisible'] else msg_data['forward_amount']
                    backward_amount = float(msg_data['backward_amount']) / config.UNIT if backward_asset_info['divisible'] else msg_data['backward_amount']
                    
                    #compose trade
                    trade = {
                        'block_index': cur_block_index,
                        'block_time': cur_block['block_time'],
                        'message_index': msg['message_index'], #secondary temporaral ordering off of when
                        'order_match_id': order_match['tx0_hash'] + order_match['tx1_hash'],
                        'base_asset': base_asset,
                        'quote_asset': quote_asset,
                        'base_asset_amount': forward_amount if msg_data['forward_asset'] == base_asset else backward_amount,
                        'quote_asset_amount': backward_amount if msg_data['forward_asset'] == base_asset else forward_amount
                    }
                    trade['unit_price'] = float(decimal.Decimal(trade['quote_asset_amount'] / trade['base_asset_amount']
                        ).quantize(decimal.Decimal('.00000000'), rounding=decimal.ROUND_UP))
                    trade['unit_price_inverse'] = float(decimal.Decimal(trade['base_asset_amount'] / trade['quote_asset_amount']
                        ).quantize(decimal.Decimal('.00000000'), rounding=decimal.ROUND_UP))
                    #^ this may be needlessly complex (we need to convert back to floats because mongo stores floats natively)

                    mongo_db.trades.insert(trade)
                    logging.info("Procesed Trade from tx %s :: %s" % (msg['message_index'], trade))
                    
                #if we're catching up beyond 10 blocks out, make sure not to send out any socket.io events, as to not flood
                # on a resync (as we may give a 525 to kick the logged in clients out, but we can't guarantee that the
                # socket.io connection will always be severed as well??)
                if last_processed_block['block_index'] - my_latest_block['block_index'] < 10: #>= max likely reorg size we'd ever see
                    #for each new entity, send out a message via socket.io
                    event = {
                        'message_index': msg['message_index'],
                        'event': msg['category'],
                        'block_time': cur_block['block_time'], #epoch ts
                        'block_time_str': cur_block['block_time_str'],
                        'msg': msg_data
                    }
                    to_socketio_queue.put(event)
            
            #block successfully processed, track this in our DB
            new_block = {
                'block_index': cur_block_index,
                'block_time': cur_block['block_time'],
                'block_hash': cur_block['block_hash'],
            }
            mongo_db.processed_blocks.insert(new_block)
            my_latest_block = new_block
        elif my_latest_block['block_index'] > last_processed_block['block_index']:
            #we have stale blocks (i.e. most likely a reorg happened in counterpartyd)
            config.CAUGHT_UP = False
            logging.warn("Ahead of counterpartyd with block indexes. Pruning from block %i back to %i ..." % (
                my_latest_block['block_index'], last_processed_block['block_index']))
            prune_my_stale_blocks(last_processed_block['block_index'], last_processed_block['block_time'])
            #NOTE that is is only the first step, that only gets our block_index equal to counterpartyd's current block
            # index... after this, we will cycle us back to querying get_running_info, which will
            # return the last X block indexes... from that, we can detect if there was a reorg that we need to further
            # prune back for
        else:
            #...we may be caught up (to counterpartyd), but counterpartyd may not be (to the blockchain). And if it isn't, we aren't
            config.CAUGHT_UP = running_info['db_caught_up']
            time.sleep(2) #counterwalletd itself is at least caught up, wait a bit to query again for the latest block from cpd


if __name__ == '__main__':
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(prog='counterwalletd', description='Counterwallet daemon. Works with counterpartyd')
    parser.add_argument('-V', '--version', action='version', version="counterwalletd v%s" % config.VERSION)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='sets log level to DEBUG instead of WARNING')

    parser.add_argument('--testnet', action='store_true', help='use Bitcoin testnet addresses and block numbers')
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
    mongo_db.processed_blocks.ensure_index('block_index', unique=True)
    mongo_db.preferences.ensure_index('wallet_id', unique=True)
    mongo_db.chat_handles.ensure_index('wallet_id', unique=True)
    mongo_db.chat_handles.ensure_index('handle', unique=True)
    mongo_db.tracked_assets.ensure_index('asset', unique=True)
    mongo_db.tracked_assets.ensure_index([
        ("owner", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
    ])
    mongo_db.trades.ensure_index([
        ("base_asset", pymongo.ASCENDING),
        ("quote_asset", pymongo.ASCENDING),
        ("block_time", pymongo.ASCENDING)
    ])
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
    
    logging.info("Starting up socket.io server (counterpartyd event feed)...")
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

    logging.info("Starting up counterpartyd block poller...")
    gevent.spawn(poll_counterpartyd, mongo_db, to_socketio_queue)

    logging.info("Starting up RPC API handler...")
    api.serve_api(mongo_db, redis_client)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
