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

import pymongo
import gevent

from lib import config, util, events, blockchain, util_bitcoin
from lib.components import assets, betting

D = decimal.Decimal

def process_cpd_blockfeed(zmq_publisher_eventfeed):
    LATEST_BLOCK_INIT = {'block_index': config.BLOCK_FIRST, 'block_time': None, 'block_hash': None}
    mongo_db = config.mongo_db

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
        
    def prune_my_stale_blocks(max_block_index):
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

        config.CAUGHT_UP = False
        latest_block = mongo_db.processed_blocks.find_one({"block_index": max_block_index}) or LATEST_BLOCK_INIT
        return latest_block
    
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


    config.CURRENT_BLOCK_INDEX = 0 #initialize (last processed block index -- i.e. currently active block)
    config.LAST_MESSAGE_INDEX = -1 #initialize (last processed message index)
    config.BLOCKCHAIN_SERVICE_LAST_BLOCK = 0 #simply for printing/alerting purposes
    config.CAUGHT_UP_STARTED_EVENTS = False
    #^ set after we are caught up and start up the recurring events that depend on us being caught up with the blockchain 
    
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
        my_latest_block = LATEST_BLOCK_INIT
    else:
        app_config = app_config[0]
        #get the last processed block out of mongo
        my_latest_block = mongo_db.processed_blocks.find_one(sort=[("block_index", pymongo.DESCENDING)]) or LATEST_BLOCK_INIT
        #remove any data we have for blocks higher than this (would happen if counterblockd or mongo died
        # or errored out while processing a block)
        my_latest_block = prune_my_stale_blocks(my_latest_block['block_index'])

    #start polling counterpartyd for new blocks    
    while True:
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
        
        #wipe our state data if necessary, if counterpartyd has moved on to a new DB version
        wipeState = False
        updatePrefs = False
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
            my_latest_block = LATEST_BLOCK_INIT
            config.CAUGHT_UP = False #You've Come a Long Way, Baby
        
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
                cur_block = util.call_jsonrpc_api("get_block_info",
                    {'block_index': cur_block_index}, abort_on_error=True)['result']
            except Exception, e:
                logging.warn(str(e) + " Waiting 3 seconds before trying again...")
                time.sleep(3)
                continue
            cur_block['block_time_obj'] = datetime.datetime.utcfromtimestamp(cur_block['block_time'])
            cur_block['block_time_str'] = cur_block['block_time_obj'].isoformat()
            
            try:
                block_data = util.call_jsonrpc_api("get_messages",
                    {'block_index': cur_block_index}, abort_on_error=True)['result']
            except Exception, e:
                logging.warn(str(e) + " Waiting 5 seconds before trying again...")
                time.sleep(5)
                continue
            
            #parse out response (list of txns, ordered as they appeared in the block)
            for msg in block_data:
                msg_data = json.loads(msg['bindings'])
                
                if msg['message_index'] != config.LAST_MESSAGE_INDEX + 1 and config.LAST_MESSAGE_INDEX != -1:
                    logging.error("BUG: MESSAGE RECEIVED NOT WHAT WE EXPECTED. EXPECTED: %s, GOT: %s: %s (ALL MSGS IN get_messages PAYLOAD: %s)..." % (
                        config.LAST_MESSAGE_INDEX + 1, msg['message_index'], msg, [m['message_index'] for m in block_data]))
                    #sys.exit(1) #FOR NOW
                
                #BUG: sometimes counterpartyd seems to return OLD messages out of the message feed. deal with those
                if msg['message_index'] <= config.LAST_MESSAGE_INDEX:
                    logging.warn("BUG: IGNORED old RAW message %s: %s ..." % (msg['message_index'], msg))
                    continue
                    
                logging.info("Received message %s: %s ..." % (msg['message_index'], msg))
                
                #don't process invalid messages, but do forward them along to clients
                status = msg_data.get('status', 'valid').lower()
                if status.startswith('invalid'):
                    #(but don't forward along while we're catching up)
                    if last_processed_block['block_index'] - my_latest_block['block_index'] < config.MAX_REORG_NUM_BLOCKS:
                        event = util.decorate_message_for_feed(msg, msg_data=msg_data)
                        zmq_publisher_eventfeed.send_json(event)
                
                    config.LAST_MESSAGE_INDEX = msg['message_index']
                    continue

                #track message types, for compiling of statistics
                if msg['command'] == 'insert' \
                   and msg['category'] not in ["debits", "credits", "order_matches", "bet_matches",
                       "order_expirations", "bet_expirations", "order_match_expirations", "bet_match_expirations",
                       "rps_matches", "rps_expirations", "rps_match_expirations", "bet_match_resolutions"]:
                    mongo_db.transaction_stats.insert({
                        'block_index': cur_block_index,
                        'block_time': cur_block['block_time_obj'],
                        'message_index': msg['message_index'],
                        'category': msg['category']
                    })
                
                #HANDLE REORGS
                if msg['command'] == 'reorg':
                    logging.warn("Blockchain reorginization at block %s" % msg_data['block_index'])
                    #prune back to and including the specified message_index
                    my_latest_block = prune_my_stale_blocks(msg_data['block_index'] - 1)
                    config.CURRENT_BLOCK_INDEX = msg_data['block_index'] - 1

                    #for the current last_message_index (which could have gone down after the reorg), query counterpartyd
                    running_info = util.call_jsonrpc_api("get_running_info", abort_on_error=True)['result']
                    config.LAST_MESSAGE_INDEX = running_info['last_message_index']
                    
                    #send out the message to listening clients (but don't forward along while we're catching up)
                    if last_processed_block['block_index'] - my_latest_block['block_index'] < config.MAX_REORG_NUM_BLOCKS:
                        msg_data['_last_message_index'] = config.LAST_MESSAGE_INDEX
                        event = util.decorate_message_for_feed(msg, msg_data=msg_data)
                        zmq_publisher_eventfeed.send_json(event)
                    break #break out of inner loop
                
                #track assets
                if msg['category'] == 'issuances':
                    assets.parse_issuance(mongo_db, msg_data, cur_block_index, cur_block)
                
                #track balance changes for each address
                bal_change = None
                if msg['category'] in ['credits', 'debits',]:
                    actionName = 'credit' if msg['category'] == 'credits' else 'debit'
                    address = msg_data['address']
                    asset_info = mongo_db.tracked_assets.find_one({ 'asset': msg_data['asset'] })
                    if asset_info is None:
                        logging.warn("Credit/debit of %s where asset ('%s') does not exist. Ignoring..." % (msg_data['quantity'], msg_data['asset']))
                        continue
                    quantity = msg_data['quantity'] if msg['category'] == 'credits' else -msg_data['quantity']
                    quantity_normalized = util_bitcoin.normalize_quantity(quantity, asset_info['divisible'])

                    #look up the previous balance to go off of
                    last_bal_change = mongo_db.balance_changes.find_one({
                        'address': address,
                        'asset': asset_info['asset']
                    }, sort=[("block_index", pymongo.DESCENDING), ("_id", pymongo.DESCENDING)])
                    
                    if     last_bal_change \
                       and last_bal_change['block_index'] == cur_block_index:
                        #modify this record, as we want at most one entry per block index for each (address, asset) pair
                        last_bal_change['quantity'] += quantity
                        last_bal_change['quantity_normalized'] += quantity_normalized
                        last_bal_change['new_balance'] += quantity
                        last_bal_change['new_balance_normalized'] += quantity_normalized
                        mongo_db.balance_changes.save(last_bal_change)
                        logging.info("Procesed %s bal change (UPDATED) from tx %s :: %s" % (actionName, msg['message_index'], last_bal_change))
                        bal_change = last_bal_change
                    else: #new balance change record for this block
                        bal_change = {
                            'address': address, 
                            'asset': asset_info['asset'],
                            'block_index': cur_block_index,
                            'block_time': cur_block['block_time_obj'],
                            'quantity': quantity,
                            'quantity_normalized': quantity_normalized,
                            'new_balance': last_bal_change['new_balance'] + quantity if last_bal_change else quantity,
                            'new_balance_normalized': last_bal_change['new_balance_normalized'] + quantity_normalized if last_bal_change else quantity_normalized,
                        }
                        mongo_db.balance_changes.insert(bal_change)
                        logging.info("Procesed %s bal change from tx %s :: %s" % (actionName, msg['message_index'], bal_change))
                
                #book trades
                if (msg['category'] == 'order_matches'
                    and ((msg['command'] == 'update' and msg_data['status'] == 'completed') #for a trade with BTC involved, but that is settled (completed)
                         or ('forward_asset' in msg_data and msg_data['forward_asset'] != config.BTC and msg_data['backward_asset'] != config.BTC))): #or for a trade without BTC on either end

                    if msg['command'] == 'update' and msg_data['status'] == 'completed':
                        #an order is being updated to a completed status (i.e. a BTCpay has completed)
                        tx0_hash, tx1_hash = msg_data['order_match_id'][:64], msg_data['order_match_id'][64:] 
                        #get the order_match this btcpay settles
                        order_match = util.call_jsonrpc_api("get_order_matches",
                            {'filters': [
                             {'field': 'tx0_hash', 'op': '==', 'value': tx0_hash},
                             {'field': 'tx1_hash', 'op': '==', 'value': tx1_hash}]
                            }, abort_on_error=True)['result'][0]
                    else:
                        assert msg_data['status'] == 'completed' #should not enter a pending state for non BTC matches
                        order_match = msg_data

                    forward_asset_info = mongo_db.tracked_assets.find_one({'asset': order_match['forward_asset']})
                    backward_asset_info = mongo_db.tracked_assets.find_one({'asset': order_match['backward_asset']})
                    assert forward_asset_info and backward_asset_info
                    base_asset, quote_asset = util.assets_to_asset_pair(order_match['forward_asset'], order_match['backward_asset'])
                    
                    #don't create trade records from order matches with BTC that are under the dust limit
                    if    (order_match['forward_asset'] == config.BTC and order_match['forward_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF) \
                       or (order_match['backward_asset'] == config.BTC and order_match['backward_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF):
                        logging.debug("Order match %s ignored due to %s under dust limit." % (order_match['tx0_hash'] + order_match['tx1_hash'], config.BTC))
                        continue

                    #take divisible trade quantities to floating point
                    forward_quantity = util_bitcoin.normalize_quantity(order_match['forward_quantity'], forward_asset_info['divisible'])
                    backward_quantity = util_bitcoin.normalize_quantity(order_match['backward_quantity'], backward_asset_info['divisible'])
                    
                    #compose trade
                    trade = {
                        'block_index': cur_block_index,
                        'block_time': cur_block['block_time_obj'],
                        'message_index': msg['message_index'], #secondary temporaral ordering off of when
                        'order_match_id': order_match['tx0_hash'] + order_match['tx1_hash'],
                        'order_match_tx0_index': order_match['tx0_index'],
                        'order_match_tx1_index': order_match['tx1_index'],
                        'order_match_tx0_address': order_match['tx0_address'],
                        'order_match_tx1_address': order_match['tx1_address'],
                        'base_asset': base_asset,
                        'quote_asset': quote_asset,
                        'base_quantity': order_match['forward_quantity'] if order_match['forward_asset'] == base_asset else order_match['backward_quantity'],
                        'quote_quantity': order_match['backward_quantity'] if order_match['forward_asset'] == base_asset else order_match['forward_quantity'],
                        'base_quantity_normalized': forward_quantity if order_match['forward_asset'] == base_asset else backward_quantity,
                        'quote_quantity_normalized': backward_quantity if order_match['forward_asset'] == base_asset else forward_quantity,
                    }
                    trade['unit_price'] = float(
                        ( D(trade['quote_quantity_normalized']) / D(trade['base_quantity_normalized']) ).quantize(
                            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
                    trade['unit_price_inverse'] = float(
                        ( D(trade['base_quantity_normalized']) / D(trade['quote_quantity_normalized']) ).quantize(
                            D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))

                    mongo_db.trades.insert(trade)
                    logging.info("Procesed Trade from tx %s :: %s" % (msg['message_index'], trade))
                
                #broadcast
                if msg['category'] == 'broadcasts':
                    betting.parse_broadcast(mongo_db, msg_data)

                #if we're catching up beyond MAX_REORG_NUM_BLOCKS blocks out, make sure not to send out any socket.io
                # events, as to not flood on a resync (as we may give a 525 to kick the logged in clients out, but we
                # can't guarantee that the socket.io connection will always be severed as well??)
                if last_processed_block['block_index'] - my_latest_block['block_index'] < config.MAX_REORG_NUM_BLOCKS:
                    #send out the message to listening clients
                    event = util.decorate_message_for_feed(msg, msg_data=msg_data)
                    zmq_publisher_eventfeed.send_json(event)

                #this is the last processed message index
                config.LAST_MESSAGE_INDEX = msg['message_index']
            
            #block successfully processed, track this in our DB
            new_block = {
                'block_index': cur_block_index,
                'block_time': cur_block['block_time_obj'],
                'block_hash': cur_block['block_hash'],
            }
            mongo_db.processed_blocks.insert(new_block)
            my_latest_block = new_block
            config.CURRENT_BLOCK_INDEX = cur_block_index
            #get the current blockchain service block
            if config.BLOCKCHAIN_SERVICE_LAST_BLOCK == 0 or config.BLOCKCHAIN_SERVICE_LAST_BLOCK - config.CURRENT_BLOCK_INDEX < config.MAX_REORG_NUM_BLOCKS:
                #update as CURRENT_BLOCK_INDEX catches up with BLOCKCHAIN_SERVICE_LAST_BLOCK and/or surpasses it (i.e. if blockchain service gets behind for some reason)
                block_height_response = blockchain.getinfo()
                config.BLOCKCHAIN_SERVICE_LAST_BLOCK = block_height_response['info']['blocks'] if block_height_response else 0
            logging.info("Block: %i (message_index height=%s) (blockchain latest block=%s)" % (config.CURRENT_BLOCK_INDEX,
                config.LAST_MESSAGE_INDEX if config.LAST_MESSAGE_INDEX != -1 else '???',
                config.BLOCKCHAIN_SERVICE_LAST_BLOCK if config.BLOCKCHAIN_SERVICE_LAST_BLOCK else '???'))

            clean_mempool_tx()

        elif my_latest_block['block_index'] > last_processed_block['block_index']:
            #we have stale blocks (i.e. most likely a reorg happened in counterpartyd)?? this shouldn't happen, as we
            # should get a reorg message. Just to be on the safe side, prune back MAX_REORG_NUM_BLOCKS blocks
            # before what counterpartyd is saying if we see this
            logging.error("Very odd: Ahead of counterpartyd with block indexes! Pruning back %s blocks to be safe." % config.MAX_REORG_NUM_BLOCKS)
            my_latest_block = prune_my_stale_blocks(last_processed_block['block_index'] - config.MAX_REORG_NUM_BLOCKS)
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
                logging.debug("Starting event timer: compile_asset_pair_market_info")
                gevent.spawn(events.compile_asset_pair_market_info)
                
                logging.debug("Starting event timer: compile_asset_market_info")
                gevent.spawn(events.compile_asset_market_info)

                logging.debug("Starting event timer: compile_extended_asset_info")
                gevent.spawn(events.compile_extended_asset_info)

                logging.debug("Starting event timer: compile_extended_feed_info")
                gevent.spawn(events.compile_extended_feed_info)

                config.CAUGHT_UP_STARTED_EVENTS = True

            publish_mempool_tx()
            time.sleep(2) #counterblockd itself is at least caught up, wait a bit to query again for the latest block from cpd
