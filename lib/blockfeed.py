"""
blockfeed: sync with and process new blocks from counterpartyd
"""
import re
import os
import sys
import json
import copy
import logging
import datetime
import decimal
import ConfigParser
import time
import itertools

import pymongo
import gevent

from lib import config, util, events, blockchain

D = decimal.Decimal 

from lib.processor.processor import MessageProcessor, BlockProcessor, CaughtUpProcessor

def prune_my_stale_blocks(max_block_index):
    mongo_db = config.mongo_db
    """called if there are any records for blocks higher than this in the database? If so, they were impartially created
       and we should get rid of them
    
    NOTE: after calling this function, you should always trigger a "continue" statement to reiterate the processing loop
    (which will get a new last_processed_block from counterpartyd and resume as appropriate)   
    """
    logging.warn("Pruning to block %i ..." % (max_block_index))        
    mongo_db.processed_blocks.remove({"block_index": {"$gt": max_block_index}})
    mongo_db.balance_changes.remove({"block_index": {"$gt": max_block_index}})
    mongo_db.trades.remove({"block_index": {"$gt": max_block_index}})
    mongo_db.asset_marketcap_history.remove({"block_index": {"$gt": max_block_index}})
    mongo_db.transaction_stats.remove({"block_index": {"$gt": max_block_index}})
    
    #to roll back the state of the tracked asset, dive into the history object for each asset that has
    # been updated on or after the block that we are pruning back to
    assets_to_prune = mongo_db.tracked_assets.find({'_at_block': {"$gt": max_block_index}})
    for asset in assets_to_prune:
        logging.info("Pruning asset %s (last modified @ block %i, pruning to state at block %i)" % (
            asset['asset'], asset['_at_block'], max_block_index))
        prev_ver = None
        while len(asset['_history']):
            prev_ver = asset['_history'].pop()
            if prev_ver['_at_block'] <= max_block_index:
                break
        if prev_ver:
            if prev_ver['_at_block'] > max_block_index:
                #even the first history version is newer than max_block_index.
                #in this case, just remove the asset tracking record itself
                mongo_db.tracked_assets.remove({'asset': asset['asset']})
            else:
                #if here, we were able to find a previous version that was saved at or before max_block_index
                # (which should be prev_ver ... restore asset's values to its values
                prev_ver['_id'] = asset['_id']
                prev_ver['_history'] = asset['_history']
                mongo_db.tracked_assets.save(prev_ver)
    config.LAST_MESSAGE_INDEX = -1
    config.CAUGHT_UP = False
    util.blockinfo_cache.clear()
    latest_block = mongo_db.processed_blocks.find_one({"block_index": max_block_index}) or config.LATEST_BLOCK_INIT
    return latest_block
        
def process_cpd_blockfeed(zmq_publisher_eventfeed):
    config.LATEST_BLOCK_INIT = {'block_index': config.BLOCK_FIRST, 'block_time': None, 'block_hash': None}
    mongo_db = config.mongo_db
    zmq_publisher_eventfeed = config.ZMQ_PUBLISHER_EVENTFEED
    
    #At least have of these Big Letter Vars are redundant with config.state and should be scrapped
    config.CURRENT_BLOCK_INDEX = 0 #initialize (last processed block index -- i.e. currently active block)
    config.LAST_MESSAGE_INDEX = -1 #initialize (last processed message index)
    config.BLOCKCHAIN_SERVICE_LAST_BLOCK = 0 #simply for printing/alerting purposes
    config.CAUGHT_UP_STARTED_EVENTS = False
    #^ set after we are caught up and start up the recurring events that depend on us being caught up with the blockchain 
    
    #enabled processor functions
    logging.debug("Enabled Message Processor Functions {0}".format(MessageProcessor.active_functions()))
    logging.debug("Enabled Block Processor Functions {0}".format(BlockProcessor.active_functions()))

    def blow_away_db():
        """boom! blow away all applicable collections in mongo"""
        mongo_db.processed_blocks.drop()
        mongo_db.tracked_assets.drop()
        mongo_db.trades.drop()
        mongo_db.balance_changes.drop()
        mongo_db.asset_market_info.drop()
        mongo_db.asset_marketcap_history.drop()
        mongo_db.pair_market_info.drop()
        mongo_db.btc_open_orders.drop()
        mongo_db.asset_extended_info.drop()
        mongo_db.transaction_stats.drop()
        mongo_db.feeds.drop()
        mongo_db.wallet_stats.drop()
        
        #create/update default app_config object
        mongo_db.app_config.update({}, {
        'db_version': config.DB_VERSION, #counterblockd database version
        'running_testnet': config.TESTNET,
        'counterpartyd_db_version_major': None,
        'counterpartyd_db_version_minor': None,
        'counterpartyd_running_testnet': None,
        'last_block_assets_compiled': config.BLOCK_FIRST, #for asset data compilation in events.py (resets on reparse as well)
        }, upsert=True)
        app_config = mongo_db.app_config.find()[0]
        
        #DO NOT DELETE preferences and chat_handles and chat_history
        
        #create XCP and BTC assets in tracked_assets
        for asset in [config.XCP, config.BTC]:
            base_asset = {
                'asset': asset,
                'owner': None,
                'divisible': True,
                'locked': False,
                'total_issued': None,
                '_at_block': config.BLOCK_FIRST, #the block ID this asset is current for
                '_history': [] #to allow for block rollbacks
            }
            mongo_db.tracked_assets.insert(base_asset)
            
        #reinitialize some internal counters
        config.CURRENT_BLOCK_INDEX = 0
        config.LAST_MESSAGE_INDEX = -1
        
        return app_config
        
    def publish_mempool_tx():
        """fetch new tx from mempool"""
        tx_hashes = []
        mempool_txs = mongo_db.mempool.find(fields={'tx_hash': True})
        for mempool_tx in mempool_txs:
            tx_hashes.append(str(mempool_tx['tx_hash']))

        params = None
        if len(tx_hashes) > 0:
            params = {
                'filters': [
                    {'field':'tx_hash', 'op': 'NOT IN', 'value': tx_hashes},
                    {'field':'category', 'op': 'IN', 'value': ['sends', 'btcpays', 'issuances', 'dividends', 'callbacks']}
                ],
                'filterop': 'AND'
            }
        new_txs = util.call_jsonrpc_api("get_mempool", params, abort_on_error=True)

        for new_tx in new_txs['result']:
            tx = {
                'tx_hash': new_tx['tx_hash'],
                'command': new_tx['command'],
                'category': new_tx['category'],
                'bindings': new_tx['bindings'],
                'timestamp': new_tx['timestamp'],
                'viewed_in_block': config.CURRENT_BLOCK_INDEX
            }
            
            mongo_db.mempool.insert(tx)
            del(tx['_id'])
            tx['_category'] = tx['category']
            tx['_message_index'] = 'mempool'
            logging.debug("Spotted mempool tx: %s" % tx)
            zmq_publisher_eventfeed.send_json(tx)
            
    def clean_mempool_tx():
        """clean mempool transactions older than MAX_REORG_NUM_BLOCKS blocks"""
        mongo_db.mempool.remove({"viewed_in_block": {"$lt": config.CURRENT_BLOCK_INDEX - config.MAX_REORG_NUM_BLOCKS}})
    
    #New Message Handling
    def parse_message(msg): 
        msg_data = json.loads(msg['bindings'])
        
        for function in MessageProcessor.active_functions():
            logging.debug('starting {}'.format(function['function']))
            cmd = function['function'](msg, msg_data) or None
            #break or *return* (?) depends on whether we want config.last_message_index to be updated
            if cmd == 'continue': break
            elif cmd == 'break': return 'break' 
            
        config.LAST_MESSAGE_INDEX = msg['message_index']

    #New Block Handling
    def parse_block(block_data): 
        config.state['cur_block']['block_time_obj'] = datetime.datetime.utcfromtimestamp(config.state['cur_block']['block_time'])
        config.state['cur_block']['block_time_str'] = config.state['cur_block']['block_time_obj'].isoformat()
        config.state['block_data'] = block_data
        cmd = None
        for msg in config.state['block_data']: 
            cmd = parse_message(msg)
            if cmd == 'break': break
        logging.debug("*config.state* {}".format(config.state))
        
        #Run Block Processor Functions
        BlockProcessor.run_active_functions()
    
        #block successfully processed, track this in our DB
        new_block = {
            'block_index': config.state['cur_block']['block_index'],
            'block_time': config.state['cur_block']['block_time_obj'],
            'block_hash': config.state['cur_block']['block_hash'],
        }
        mongo_db.processed_blocks.insert(new_block)
        config.state['my_latest_block'] = new_block
        config.CURRENT_BLOCK_INDEX = config.state['cur_block']['block_index']
        #get the current blockchain service block
        if config.BLOCKCHAIN_SERVICE_LAST_BLOCK == 0 or config.BLOCKCHAIN_SERVICE_LAST_BLOCK - config.CURRENT_BLOCK_INDEX < config.MAX_REORG_NUM_BLOCKS:
            #update as CURRENT_BLOCK_INDEX catches up with BLOCKCHAIN_SERVICE_LAST_BLOCK and/or surpasses it (i.e. if blockchain service gets behind for some reason)
            block_height_response = blockchain.getinfo()
            config.BLOCKCHAIN_SERVICE_LAST_BLOCK = block_height_response['info']['blocks'] if block_height_response else 0
        logging.info("Block: %i (message_index height=%s) (blockchain latest block=%s)" % (config.CURRENT_BLOCK_INDEX,
            config.LAST_MESSAGE_INDEX if config.LAST_MESSAGE_INDEX != -1 else '???',
            config.BLOCKCHAIN_SERVICE_LAST_BLOCK if config.BLOCKCHAIN_SERVICE_LAST_BLOCK else '???'))

        clean_mempool_tx()
    
    #grab our stored preferences, and rebuild the database if necessary
    app_config = mongo_db.app_config.find()
    assert app_config.count() in [0, 1]
    if (   app_config.count() == 0
        or config.REPARSE_FORCED
        or app_config[0]['db_version'] != config.DB_VERSION
        or app_config[0]['running_testnet'] != config.TESTNET):
        if app_config.count():
            logging.warn("counterblockd database version UPDATED (from %i to %i) or testnet setting changed (from %s to %s), or REINIT forced (%s). REBUILDING FROM SCRATCH ..." % (
                app_config[0]['db_version'], config.DB_VERSION, app_config[0]['running_testnet'], config.TESTNET, config.REPARSE_FORCED))
        else:
            logging.warn("counterblockd database app_config collection doesn't exist. BUILDING FROM SCRATCH...")
        app_config = blow_away_db()
        config.state['my_latest_block'] = config.LATEST_BLOCK_INIT
    else:
        app_config = app_config[0]
        #get the last processed block out of mongo
        config.state['my_latest_block'] = mongo_db.processed_blocks.find_one(sort=[("block_index", pymongo.DESCENDING)]) or config.LATEST_BLOCK_INIT
        #remove any data we have for blocks higher than this (would happen if counterblockd or mongo died
        # or errored out while processing a block)
        config.state['my_latest_block'] = prune_my_stale_blocks(config.state['my_latest_block']['block_index'])
    
    #avoid contacting counterpartyd (on reparse, to speed up)
    autopilot = False
    autopilot_runner = 0

    #start polling counterpartyd for new blocks
    while True:
        if not autopilot or autopilot_runner == 0:
            try:
                running_info = util.call_jsonrpc_api("get_running_info", abort_on_error=True)
                if 'result' not in running_info:
                    raise AssertionError("Could not contact counterpartyd")
                running_info = running_info['result']
            except Exception, e:
                logging.warn(str(e) + " -- Waiting 3 seconds before trying again...")
                time.sleep(3)
                continue
            if running_info['last_message_index'] == -1: #last_message_index not set yet (due to no messages in counterpartyd DB yet)
                logging.warn("No last_message_index returned. Waiting until counterpartyd has messages...")
                time.sleep(10)
                continue

            #set the last block processed by counterpartyd
            config.state['last_processed_block'] = running_info['last_block']
         
        #wipe our state data if necessary, if counterpartyd has moved on to a new DB version
        wipeState = False
        updatePrefs = False
        
        #Checking appconfig against old running info (when batch-fetching) is redundant 
        if    app_config['counterpartyd_db_version_major'] is None \
           or app_config['counterpartyd_db_version_minor'] is None \
           or app_config['counterpartyd_running_testnet'] is None:
            updatePrefs = True
        elif running_info['version_major'] != app_config['counterpartyd_db_version_major']:
            logging.warn("counterpartyd MAJOR DB version change (we built from %s, counterpartyd is at %s). Wiping our state data." % (
                app_config['counterpartyd_db_version_major'], running_info['version_major']))
            wipeState = True
            updatePrefs = True
        elif running_info['version_minor'] != app_config['counterpartyd_db_version_minor']:
            logging.warn("counterpartyd MINOR DB version change (we built from %s.%s, counterpartyd is at %s.%s). Wiping our state data." % (
                app_config['counterpartyd_db_version_major'], app_config['counterpartyd_db_version_minor'],
                running_info['version_major'], running_info['version_minor']))
            wipeState = True
            updatePrefs = True
        elif running_info.get('running_testnet', False) != app_config['counterpartyd_running_testnet']:
            logging.warn("counterpartyd testnet setting change (from %s to %s). Wiping our state data." % (
                app_config['counterpartyd_running_testnet'], running_info['running_testnet']))
            wipeState = True
            updatePrefs = True
        if wipeState:
            app_config = blow_away_db()
        if updatePrefs:
            app_config['counterpartyd_db_version_major'] = running_info['version_major'] 
            app_config['counterpartyd_db_version_minor'] = running_info['version_minor']
            app_config['counterpartyd_running_testnet'] = running_info['running_testnet']
            mongo_db.app_config.update({}, app_config)
            
            
            #reset my latest block record
            config.state['my_latest_block'] = config.LATEST_BLOCK_INIT #Also Redudant see #L229,  L223
            config.CAUGHT_UP = False #You've Come a Long Way, Baby
            
        if config.state['last_processed_block']['block_index'] is None:
            logging.warn("counterpartyd has no last processed block (probably is reparsing). Waiting 3 seconds before trying again...")
            time.sleep(3)
            continue
        if config.state['my_latest_block']['block_index'] < config.state['last_processed_block']['block_index']:
            #need to catch up
            config.CAUGHT_UP = False
            
            #Autopilot and autopilot runner are redundant
            if last_processed_block['block_index'] - my_latest_block['block_index'] > 500: #we are safely far from the tip, switch to bulk-everything
                autopilot = True
                if autopilot_runner == 0:
                    autopilot_runner = 500
                autopilot_runner -= 1
            else:
                autopilot = False
                
            cur_block_index = config.state['my_latest_block']['block_index'] + 1
            try:
                block_data = util.get_block_info_cached(cur_block_index, min(100, (config.state['last_processed_block']['block_index'] - config.state['my_latest_block']['block_index'])))
            except Exception, e:
                logging.warn(str(e) + " Waiting 3 seconds before trying again...")
                time.sleep(3)
                continue
            config.state['cur_block'] = block_data
            parse_block(block_data['_messages'])
            
            #What's this for, if it's not general to blockfeed it should be in BlockProcessor
            if last_processed_block['block_index'] - cur_block_index <= config.MAX_REORG_NUM_BLOCKS: #only when we are near the tip
                util.clean_block_cache(cur_block_index)

        elif config.state['my_latest_block']['block_index'] > config.state['last_processed_block']['block_index']:
            # should get a reorg message. Just to be on the safe side, prune back MAX_REORG_NUM_BLOCKS blocks
            # before what counterpartyd is saying if we see this
            logging.error("Very odd: Ahead of counterpartyd with block indexes! Pruning back %s blocks to be safe." % config.MAX_REORG_NUM_BLOCKS)
            config.state['my_latest_block'] = prune_my_stale_blocks(config.state['last_processed_block']['block_index'] - config.MAX_REORG_NUM_BLOCKS)
        else:
            #...we may be caught up (to counterpartyd), but counterpartyd may not be (to the blockchain). And if it isn't, we aren't
            config.CAUGHT_UP = running_info['db_caught_up']
            
            #this logic here will cover a case where we shut down counterblockd, then start it up again quickly...
            # in that case, there are no new blocks for it to parse, so LAST_MESSAGE_INDEX would otherwise remain 0.
            # With this logic, we will correctly initialize LAST_MESSAGE_INDEX to the last message ID of the last processed block
            if config.LAST_MESSAGE_INDEX == -1 or config.CURRENT_BLOCK_INDEX == 0:
                if config.LAST_MESSAGE_INDEX == -1: config.LAST_MESSAGE_INDEX = running_info['last_message_index']
                if config.CURRENT_BLOCK_INDEX == 0: config.CURRENT_BLOCK_INDEX = running_info['last_block']['block_index']
                logging.info("Detected blocks caught up on startup. Setting last message idx to %s, current block index to %s ..." % (
                    config.LAST_MESSAGE_INDEX, config.CURRENT_BLOCK_INDEX))
            
            if config.CAUGHT_UP and not config.CAUGHT_UP_STARTED_EVENTS:
                #start up recurring events that depend on us being fully caught up with the blockchain to run
                CaughtUpProcessor.run_active_functions()
                
                config.CAUGHT_UP_STARTED_EVENTS = True

            publish_mempool_tx()
            time.sleep(2) #counterblockd itself is at least caught up, wait a bit to query again for the latest block from cpd
