"""
blockfeed: sync with and process new blocks from counterpartyd
"""
import os
import json
import copy
import logging
import datetime
import decimal
import ConfigParser
import time

import pymongo

from lib import (config, util)

def process_cpd_blockfeed(mongo_db, to_socketio_queue):
    def blow_away_db(prefs):
        #boom! blow away all collections in mongo
        mongo_db.processed_blocks.remove()
        mongo_db.balance_changes.remove()
        mongo_db.trades.remove()
        mongo_db.tracked_assets.remove()
        prefs['db_version'] = config.DB_VERSION
        mongo_db.prefs.update({}, prefs)
        
        #create XCP and BTC assets in tracked_assets
        for asset in ['XCP', 'BTC']:
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
        
        return prefs
        
    def prune_my_stale_blocks(max_block_index, max_block_time):
        """called if there are any records for blocks higher than this in the database? If so, they were impartially created
           and we should get rid of them"""
        mongo_db.balance_changes.remove({"block_index": {"$gt": max_block_index}})
        mongo_db.trades.remove({"block_index": {"$gt": max_block_index}})
        
        #to roll back the state of the tracked asset, dive into the history object for each asset that has
        # been updated on or after the block that we are pruning back to
        assets_to_prune = mongo_db.tracked_assets.find({'_at_block': {"$gt": max_block_index}})
        for asset in assets_to_prune:
            logging.info("Pruning asset %s (last modified @ block %i, pruning to state at block %i)" % (
                asset['asset'], asset['_at_block'], max_block_index))
            while len(asset['_history']):
                prev_ver = asset['_history'].pop()
                if prev_ver['_at_block'] <= max_block_index:
                    break
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

    #grab our stored preferences
    prefs_created = False
    prefs = mongo_db.prefs.find()
    assert prefs.count() in [0, 1]
    if prefs.count() == 0 or config.REINIT_FORCED:
        mongo_db.prefs.update({}, { #create/update default prefs object
        'db_version': config.DB_VERSION, #counterwalletd database version
        'running_testnet': config.TESTNET,
        'counterpartyd_db_version_major': None,
        'counterpartyd_db_version_minor': None,
        'counterpartyd_running_testnet': None,
        }, upsert=True)
        prefs = mongo_db.prefs.find()[0]
        prefs_created = True
    else:
        prefs = prefs[0]
        
    #get the last processed block out of mongo
    my_latest_block = mongo_db.processed_blocks.find_one(sort=[("block_index", pymongo.DESCENDING)])
    if not my_latest_block:
        my_latest_block = {'block_index': config.BLOCK_FIRST, 'block_time': None, 'block_hash': None}

    #see if DB version has increased and rebuild if so
    if prefs_created or config.REINIT_FORCED or prefs['db_version'] != config.DB_VERSION or prefs['running_testnet'] != config.TESTNET:
        logging.warn("counterwalletd database version UPDATED (from %i to %i) or testnet setting changed (from %s to %s), or REINIT forced (%s). REBUILDING FROM SCRATCH ..." % (
            prefs['db_version'], config.DB_VERSION, prefs['running_testnet'], config.TESTNET, config.REINIT_FORCED))
        prefs = blow_away_db(prefs)
        my_latest_block = {'block_index': config.BLOCK_FIRST, 'block_time': None, 'block_hash': None}
    else:
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
        wipeState = False
        updatePrefs = False
        if    prefs['counterpartyd_db_version_major'] is None \
           or prefs['counterpartyd_db_version_minor'] is None \
           or prefs['counterpartyd_running_testnet'] is None:
            updatePrefs = True
        elif running_info['db_version_major'] != prefs['counterpartyd_db_version_major']:
            logging.warn("counterpartyd MAJOR DB version change (we built from %s, counterpartyd is at %s). Wiping our state data." % (
                prefs['counterpartyd_db_version_major'], running_info['db_version_major']))
            wipeState = True
            updatePrefs = True
        elif running_info['db_version_minor'] != prefs['counterpartyd_db_version_minor']:
            logging.warn("counterpartyd MINOR DB version change (we built from %s.%s, counterpartyd is at %s.%s). Wiping our state data." % (
                prefs['counterpartyd_db_version_major'], prefs['counterpartyd_db_version_minor'],
                running_info['db_version_major'], running_info['db_version_minor']))
            wipeState = True
            updatePrefs = True
        elif running_info['running_testnet'] != prefs['counterpartyd_running_testnet']:
            logging.warn("counterpartyd testnet setting change (from %s to %s). Wiping our state data." % (
                prefs['counterpartyd_running_testnet'], running_info['running_testnet']))
            wipeState = True
            updatePrefs = True
        if wipeState:
            prefs = blow_away_db(prefs)
        if updatePrefs:
            prefs['counterpartyd_db_version_major'] = running_info['db_version_major'] 
            prefs['counterpartyd_db_version_minor'] = running_info['db_version_minor']
            prefs['counterpartyd_running_testnet'] = running_info['running_testnet']
            mongo_db.prefs.update({}, prefs)
            
            #reset my latest block record
            my_latest_block = {'block_index': config.BLOCK_FIRST, 'block_time': None, 'block_hash': None}
            config.CAUGHT_UP = False #You've Come a Long Way, Baby
        
        
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
                
                #keep out the riff raff...
                if msg_data.get('validity', 'valid').lower() != 'valid':
                    continue
                
                #track assets
                if msg['category'] == 'issuances':
                    tracked_asset = mongo_db.tracked_assets.find_one(
                        {'asset': msg_data['asset']}, {'_id': 0, '_history': 0})
                    #^ pulls the tracked asset without the _id and history fields. This may be None
                    
                    if msg_data['amount'] == 0: #lock asset
                        assert tracked_asset
                        mongo_db.tracked_assets.update(
                            {'asset': msg_data['asset']},
                            {"$set": {'locked': True, '_at_block': cur_block_index},
                             "$push": {'_history': tracked_asset } }, upsert=False)
                    elif msg_data['transfer']: #transfer asset
                        assert tracked_asset
                        mongo_db.tracked_assets.update(
                            {'asset': msg_data['asset']},
                            {"$set": {'owner': msg_data['issuer'], '_at_block': cur_block_index},
                             "$push": {'_history': tracked_asset } }, upsert=False)
                    else: #issue new asset or issue addition qty of an asset
                        if not tracked_asset:
                            tracked_asset = {
                                'asset': msg_data['asset'],
                                'owner': msg_data['issuer'],
                                'divisible': msg_data['divisible'],
                                'locked': False,
                                'total_issued': msg_data['amount'],
                                '_at_block': cur_block_index, #the block ID this asset is current for
                                #(if there are multiple asset tracked changes updates in a single block for the same
                                # asset, the last one with _at_block == that block id in the history array is the
                                # final version for that asset at that block
                                '_history': [] #to allow for block rollbacks
                            }
                            mongo_db.tracked_assets.insert(tracked_asset)
                        else:
                            assert tracked_asset
                            mongo_db.tracked_assets.update(
                                {'asset': msg_data['asset']},
                                {"$set": {'_at_block': cur_block_index},
                                 "$inc": {'total_issued': msg_data['amount']},
                                 "$push": {'_history': tracked_asset} }, upsert=False)
                
                #track balance changes for each address
                if msg['category'] in ['credits', 'debits']:
                    amount = msg_data['amount'] if msg['category'] == 'credits' else -msg_data['amount']                    

                    #look up the previous balance to go off of
                    last_bal_change = mongo_db.balance_changes.find_one({
                        'address': msg_data['address'],
                        'asset': msg_data['asset']
                    }, sort=[("block_time", pymongo.DESCENDING)])
                    #print("GOT last_bal_change", last_bal_change, cur_block_index)
                    
                    if     last_bal_change \
                       and last_bal_change['block_index'] == cur_block_index:
                        #modify this record, as we want at most one entry per block index for each (address, asset) pair
                        print "bla", msg_data['asset'], amount, last_bal_change['amount'], last_bal_change['new_balance']
                        last_bal_change['amount'] += amount 
                        last_bal_change['new_balance'] += amount
                        mongo_db.balance_changes.save(last_bal_change)
                        logging.info("Procesed BalChange (UPDATED) from tx %s :: %s" % (msg['message_index'], last_bal_change))
                    else: #new balance change record for this block
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
                    if msg['category'] == 'btcpays':
                        tx0_hash, tx1_hash = msg_data['order_match_id'][:64], msg_data['order_match_id'][64:] 
                        #get the order_match this btcpay settles
                        order_match = util.call_jsonrpc_api("get_order_matches",
                            [{'field': 'tx0_hash', 'op': '==', 'value': tx0_hash},
                             {'field': 'tx1_hash', 'op': '==', 'value': tx1_hash}], abort_on_error=True)['result'][0]
                    else:
                        order_match = msg_data

                    forward_asset_info = util.call_jsonrpc_api("get_asset_info", [order_match['forward_asset'],], abort_on_error=True)['result']
                    backward_asset_info = util.call_jsonrpc_api("get_asset_info", [order_match['backward_asset'],], abort_on_error=True)['result']
                    
                    base_asset, quote_asset = util.assets_to_asset_pair(order_match['forward_asset'], order_match['backward_asset'])
                    #print "BASE",base_asset,"QUOTE", quote_asset
                    
                    #take divisible trade amounts to floating point
                    forward_amount = float(order_match['forward_amount']) / config.UNIT if forward_asset_info['divisible'] else order_match['forward_amount']
                    backward_amount = float(order_match['backward_amount']) / config.UNIT if backward_asset_info['divisible'] else order_match['backward_amount']
                    
                    #compose trade
                    trade = {
                        'block_index': cur_block_index,
                        'block_time': cur_block['block_time'],
                        'message_index': msg['message_index'], #secondary temporaral ordering off of when
                        'order_match_id': order_match['tx0_hash'] + order_match['tx1_hash'],
                        'order_match_tx0_index': order_match['tx0_index'],
                        'order_match_tx1_index': order_match['tx1_index'],
                        #'order_match_tx0_address': order_match['tx0_address'],
                        #'order_match_tx1_address': order_match['tx1_address'],
                        'base_asset': base_asset,
                        'quote_asset': quote_asset,
                        'base_asset_amount': forward_amount if order_match['forward_asset'] == base_asset else backward_amount,
                        'quote_asset_amount': backward_amount if order_match['forward_asset'] == base_asset else forward_amount
                    }
                    trade['unit_price'] = float(decimal.Decimal(trade['quote_asset_amount'] / trade['base_asset_amount']
                        ).quantize(decimal.Decimal('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
                    trade['unit_price_inverse'] = float(decimal.Decimal(trade['base_asset_amount'] / trade['quote_asset_amount']
                        ).quantize(decimal.Decimal('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
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
