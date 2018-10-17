"""
blockfeed: sync with and process new blocks from counterparty-server
"""
import re
import os
import sys
import json
import copy
import logging
import datetime
import decimal
import configparser
import time
import itertools
import pymongo
import gevent

from counterblock.lib import config, util, blockchain, cache, database
from counterblock.lib.processor import MessageProcessor, MempoolMessageProcessor, BlockProcessor, CaughtUpProcessor

D = decimal.Decimal
logger = logging.getLogger(__name__)


def fuzzy_is_caught_up():
    """We don't want to give users 525 errors or login errors if counterblockd/counterparty-server is in the process of
    getting caught up, but we DO if counterblockd is either clearly out of date with the blockchain, or reinitializing its database"""
    if not config.state['caught_up']:
        return False
    if not config.state['cp_backend_block_index'] or not config.state['my_latest_block']['block_index']:
        return False
    if config.state['my_latest_block']['block_index'] < config.state['cp_backend_block_index'] - 1:  # "fuzzy" part
        return False
    return True


def process_cp_blockfeed():
    # initialize state
    config.state['cur_block'] = {'block_index': 0, }  # block being currently processed
    config.state['my_latest_block'] = {'block_index': 0}  # last block that was successfully processed by counterblockd
    config.state['last_message_index'] = -1  # initialize (last processed message index)
    config.state['cp_latest_block_index'] = 0  # last block that was successfully processed by counterparty
    config.state['cp_backend_block_index'] = 0  # the latest block height as reported by the cpd blockchain backend
    config.state['cp_caught_up'] = False  # whether counterparty-server is caught up to the backend (e.g. bitcoind)
    config.state['caught_up_started_events'] = False
    #^ set after we are caught up and start up the recurring events that depend on us being caught up with the blockchain

    # enabled processor functions
    logger.debug("Enabled Message Processor Functions {0}".format(MessageProcessor.active_functions()))
    logger.debug("Enabled Block Processor Functions {0}".format(BlockProcessor.active_functions()))

    def publish_mempool_tx():
        """fetch new tx from mempool"""
        mempool_txs = config.mongo_db.mempool.find(projection={'tx_hash': True})
        tx_hashes = {t['tx_hash'] for t in mempool_txs}

        params = { # get latest 1000 entries from mempool
            'order_by': 'timestamp',
            'order_dir': 'DESC'
        }
        new_txs = util.jsonrpc_api("get_mempool", params, abort_on_error=True, use_cache=False)
        num_skipped_tx = 0
        if new_txs:
            for new_tx in new_txs['result']:
                # skip if it's already in our mempool table
                if new_tx['tx_hash'] in tx_hashes:
                    num_skipped_tx += 1
                    continue

                tx = {
                    'tx_hash': new_tx['tx_hash'],
                    'command': new_tx['command'],
                    'category': new_tx['category'],
                    'bindings': new_tx['bindings'],
                    'timestamp': new_tx['timestamp'],
                    'viewed_in_block': config.state['my_latest_block']['block_index']
                }
                config.mongo_db.mempool.insert(tx)
                del(tx['_id'])
                tx['_category'] = tx['category']
                tx['_message_index'] = 'mempool'
                logger.debug("Spotted mempool tx: %s" % tx)
                for function in MempoolMessageProcessor.active_functions():
                    logger.debug('starting {} (mempool)'.format(function['function']))
                    # TODO: Better handling of double parsing
                    try:
                        result = function['function'](tx, json.loads(tx['bindings'])) or None
                    except pymongo.errors.DuplicateKeyError as e:
                        logging.exception(e)
                    if result == 'ABORT_THIS_MESSAGE_PROCESSING' or result == 'continue':
                        break
                    elif result:
                        raise Exception(
                            "Message processor returned unknown code -- processor: '%s', result: '%s'" %
                            (function, result))
        logger.debug("Mempool refresh: {} entries retrieved from counterparty-server, {} new".format(len(new_txs['result']) if new_txs else '??', (len(new_txs['result']) - num_skipped_tx) if new_txs else '??'))

    def clean_mempool_tx():
        """clean mempool transactions older than MAX_REORG_NUM_BLOCKS blocks"""
        config.mongo_db.mempool.remove(
            {"viewed_in_block": {"$lt": config.state['my_latest_block']['block_index'] - config.MAX_REORG_NUM_BLOCKS}})

    def parse_message(msg):
        msg_data = json.loads(msg['bindings'])
        logger.debug("Received message %s: %s ..." % (msg['message_index'], msg))

        # out of order messages should not happen (anymore), but just to be sure
        if msg['message_index'] != config.state['last_message_index'] + 1 and config.state['last_message_index'] != -1:
            raise Exception("Message index mismatch. Next message's message_index: %s, last_message_index: %s" % (
                msg['message_index'], config.state['last_message_index']))

        for function in MessageProcessor.active_functions():
            logger.debug('MessageProcessor: starting {}'.format(function['function']))
            # TODO: Better handling of double parsing
            try:
                result = function['function'](msg, msg_data) or None
            except pymongo.errors.DuplicateKeyError as e:
                logging.exception(e)

            if result in (
                    'ABORT_THIS_MESSAGE_PROCESSING', 'continue',  # just abort further MessageProcessors for THIS message
                    'ABORT_BLOCK_PROCESSING'):  # abort all further block processing, including that of all messages in the block
                break
            elif result not in (True, False, None):
                raise Exception(
                    "Message processor returned unknown code -- processor: '%s', result: '%s'" %
                    (function, result))

        config.state['last_message_index'] = msg['message_index']
        return 'ABORT_BLOCK_PROCESSING' if result == 'ABORT_BLOCK_PROCESSING' else None

    def parse_block(block_data):
        config.state['cur_block'] = block_data
        config.state['cur_block']['block_time_obj'] \
            = datetime.datetime.utcfromtimestamp(config.state['cur_block']['block_time'])
        config.state['cur_block']['block_time_str'] = config.state['cur_block']['block_time_obj'].isoformat()

        for msg in config.state['cur_block']['_messages']:
            result = parse_message(msg)
            if result == 'ABORT_BLOCK_PROCESSING':  # reorg
                return False

        # run block processor Functions
        BlockProcessor.run_active_functions()
        # block successfully processed, track this in our DB
        new_block = {
            'block_index': config.state['cur_block']['block_index'],
            'block_time': config.state['cur_block']['block_time_obj'],
            'block_hash': config.state['cur_block']['block_hash'],
        }
        config.mongo_db.processed_blocks.insert(new_block)

        config.state['my_latest_block'] = new_block

        if config.state['my_latest_block']['block_index'] % 10 == 0:  # every 10 blocks print status
            root_logger = logging.getLogger()
            root_logger.setLevel(logging.INFO)
        logger.info("Block: %i of %i [message height=%s]" % (
            config.state['my_latest_block']['block_index'],
            config.state['cp_backend_block_index']
            if config.state['cp_backend_block_index'] else '???',
            config.state['last_message_index'] if config.state['last_message_index'] != -1 else '???'))
        if config.state['my_latest_block']['block_index'] % 10 == 0:  # every 10 blocks print status
            root_logger.setLevel(logging.WARNING)

        return True

    # grab our stored preferences, and rebuild the database if necessary
    app_config = config.mongo_db.app_config.find()
    assert app_config.count() in [0, 1]
    if(app_config.count() == 0 or
       app_config[0]['db_version'] != config.DB_VERSION or
       app_config[0]['running_testnet'] != config.TESTNET or
       app_config[0]['running_regtest'] != config.REGTEST):
        if app_config.count():
            logger.warn("counterblockd database version UPDATED (from %i to %i) or testnet/regtest setting changed (from %s to %s, or %s to %s). REBUILDING FROM SCRATCH ..." % (
                app_config[0]['db_version'], config.DB_VERSION, app_config[0]['running_testnet'],
                config.TESTNET, app_config[0]['running_regtest'], config.REGTEST))
        else:
            logger.warn("counterblockd database app_config collection doesn't exist. BUILDING FROM SCRATCH...")
        app_config = database.init_reparse()
    else:
        app_config = app_config[0]
        # get the last processed block out of mongo
        my_latest_block = config.mongo_db.processed_blocks.find_one(sort=[("block_index", pymongo.DESCENDING)])
        if my_latest_block:
            # remove any data we have for blocks higher than this (would happen if counterblockd or mongo died
            # or errored out while processing a block)
            database.rollback(my_latest_block['block_index'])
        else:
            # no block state in the database yet
            config.state['my_latest_block'] = config.LATEST_BLOCK_INIT

    # avoid contacting counterparty-server (on reparse, to speed up)
    autopilot = False
    autopilot_runner = 0
    iteration = 0

    if config.IS_REPARSING:
        reparse_start = time.time()
        root_logger = logging.getLogger()
        root_logger_level = root_logger.getEffectiveLevel()
        root_logger.setLevel(logging.WARNING)

    # start polling counterparty-server for new blocks
    cp_running_info = None
    while True:
        iteration += 1
        if iteration % 10 == 0:
            logger.info(
                "Heartbeat (%s, block: %s, caught up: %s)"
                % (iteration, config.state['my_latest_block']['block_index'], fuzzy_is_caught_up()))
        logger.debug(
            "iteration: ap %s/%s, cp_latest_block_index: %s, my_latest_block: %s" % (autopilot, autopilot_runner,
                                                                                     config.state['cp_latest_block_index'], config.state['my_latest_block']['block_index']))

        if not autopilot or autopilot_runner == 0:
            try:
                cp_running_info = util.jsonrpc_api("get_running_info", abort_on_error=True, use_cache=False)['result']
            except Exception as e:
                logger.warn("Cannot contact counterparty-server (via get_running_info): {}".format(e))
                time.sleep(3)
                continue

        # wipe our state data if necessary, if counterparty-server has moved on to a new DB version
        wipeState = False
        updatePrefs = False

        # Checking appconfig against old running info (when batch-fetching) is redundant
        if    app_config['counterpartyd_db_version_major'] is None \
           or app_config['counterpartyd_db_version_minor'] is None \
           or app_config['counterpartyd_running_testnet'] is None \
           or app_config['counterpartyd_running_regtest'] is None:
            logger.info("Updating version info from counterparty-server")
            updatePrefs = True
        elif cp_running_info['version_major'] != app_config['counterpartyd_db_version_major']:
            logger.warn(
                "counterparty-server MAJOR DB version change (we built from %s, counterparty-server is at %s). Wiping our state data." % (
                    app_config['counterpartyd_db_version_major'], cp_running_info['version_major']))
            wipeState = True
            updatePrefs = True
        elif cp_running_info['version_minor'] != app_config['counterpartyd_db_version_minor']:
            logger.warn(
                "counterparty-server MINOR DB version change (we built from %s.%s, counterparty-server is at %s.%s). Wiping our state data." % (
                    app_config['counterpartyd_db_version_major'], app_config['counterpartyd_db_version_minor'],
                    cp_running_info['version_major'], cp_running_info['version_minor']))
            wipeState = True
            updatePrefs = True
        elif cp_running_info.get('running_testnet', False) != app_config['counterpartyd_running_testnet']:
            logger.warn("counterparty-server testnet setting change (from %s to %s). Wiping our state data." % (
                app_config['counterpartyd_running_testnet'], cp_running_info['running_testnet']))
            wipeState = True
            updatePrefs = True
        elif cp_running_info.get('running_regtest', False) != app_config['counterpartyd_running_regtest']:
            logger.warn("counterparty-server regtest setting change (from %s to %s). Wiping our state data." % (
                app_config['counterpartyd_running_regtest'], cp_running_info['running_regtest']))
            wipeState = True
            updatePrefs = True
        if wipeState:
            app_config = database.reset_db_state()
        if updatePrefs:
            app_config['counterpartyd_db_version_major'] = cp_running_info['version_major']
            app_config['counterpartyd_db_version_minor'] = cp_running_info['version_minor']
            app_config['counterpartyd_running_testnet'] = cp_running_info['running_testnet']
            app_config['counterpartyd_running_regtest'] = cp_running_info['running_regtest']
            config.mongo_db.app_config.update({}, app_config)
            # reset my latest block record
            config.state['my_latest_block'] = config.LATEST_BLOCK_INIT
            config.state['caught_up'] = False

        # work up to what block counterpartyd is at
        try:
            if cp_running_info['last_block']:  # should normally exist, unless counterparty-server had an error getting it
                assert cp_running_info['last_block']['block_index']
                config.state['cp_latest_block_index'] = cp_running_info['last_block']['block_index']
            elif cp_running_info['db_caught_up']:
                config.state['cp_latest_block_index'] = cp_running_info['bitcoin_block_count']
            else:
                assert False
        except:
            logger.warn(
                "counterparty-server not returning a valid last processed block (probably is reparsing or was just restarted)."
                + " Waiting 3 seconds before trying again... (Data returned: %s, we have: %s)" % (
                    cp_running_info, config.state['cp_latest_block_index']))
            time.sleep(3)
            continue

        config.state['cp_backend_block_index'] = cp_running_info['bitcoin_block_count']
        config.state['cp_caught_up'] = cp_running_info['db_caught_up']

        if config.state['my_latest_block']['block_index'] < config.state['cp_latest_block_index']:
            # need to catch up
            config.state['caught_up'] = False

            # TODO: Autopilot and autopilot runner are redundant
            if config.state['cp_latest_block_index'] - config.state['my_latest_block']['block_index'] > 500:
                # we are safely far from the tip, switch to bulk-everything
                autopilot = True
                if autopilot_runner == 0:
                    autopilot_runner = 500
                autopilot_runner -= 1
            else:
                autopilot = False

            cur_block_index = config.state['my_latest_block']['block_index'] + 1
            try:
                block_data = cache.get_block_info(
                    cur_block_index,
                    prefetch=min(100, (config.state['cp_latest_block_index'] - config.state['my_latest_block']['block_index'])),
                    min_message_index=config.state['last_message_index'] + 1 if config.state['last_message_index'] != -1 else None)
            except Exception as e:
                logger.warn(str(e) + " Waiting 3 seconds before trying again...")
                time.sleep(3)
                continue

            try:
                result = parse_block(block_data)
            except Exception as e:  # if anything bubbles up
                logger.exception("Unhandled exception while processing block. Rolling back, waiting 3 seconds and retrying. Error was: %s" % e)

                # counterparty-server might have gone away...
                my_latest_block = config.mongo_db.processed_blocks.find_one(sort=[("block_index", pymongo.DESCENDING)])
                if my_latest_block:
                    database.rollback(my_latest_block['block_index'])

                # disable autopilot this next iteration to force us to check up against counterparty-server
                # (it will be re-enabled later on in that same iteration if we are far enough from the tip)
                autopilot = False

                time.sleep(3)
                continue
            if result is False:  # reorg, or block processing otherwise not completed
                autopilot = False

            if config.state['cp_latest_block_index'] - cur_block_index < config.MAX_REORG_NUM_BLOCKS:  # only when we are near the tip
                clean_mempool_tx()
        elif config.state['my_latest_block']['block_index'] > config.state['cp_latest_block_index']:
            # should get a reorg message. Just to be on the safe side, prune back MAX_REORG_NUM_BLOCKS blocks
            # before what counterpartyd is saying if we see this
            logger.error(
                "Very odd: Ahead of counterparty-server with block indexes! Pruning back %s blocks to be safe."
                % config.MAX_REORG_NUM_BLOCKS)
            database.rollback(config.state['cp_latest_block_index'] - config.MAX_REORG_NUM_BLOCKS)
        else:
            if config.IS_REPARSING:
                # restore logging state
                root_logger.setLevel(root_logger_level)
                # print out how long the reparse took
                reparse_end = time.time()
                logger.info("Reparse took {:.3f} minutes.".format((reparse_end - reparse_start) / 60.0))
                config.IS_REPARSING = False

            if config.QUIT_AFTER_CAUGHT_UP:
                sys.exit(0)

            # ...we may be caught up (to counterpartyd), but counterpartyd may not be (to the blockchain). And if it isn't, we aren't
            config.state['caught_up'] = config.state['cp_caught_up']

            # this logic here will cover a case where we shut down counterblockd, then start it up again quickly...
            # in that case, there are no new blocks for it to parse, so config.state['last_message_index'] would otherwise remain 0.
            # With this logic, we will correctly initialize config.state['last_message_index'] to the last message ID of the last processed block
            if config.state['last_message_index'] == -1 or config.state['my_latest_block']['block_index'] == 0:
                if config.state['last_message_index'] == -1:
                    config.state['last_message_index'] = cp_running_info['last_message_index']
                if config.state['my_latest_block']['block_index'] == 0:
                    config.state['my_latest_block']['block_index'] = cp_running_info['last_block']['block_index']
                logger.info("Detected blocks caught up on startup. Setting last message idx to %s, current block index to %s ..." % (
                    config.state['last_message_index'], config.state['my_latest_block']['block_index']))

            if config.state['caught_up'] and not config.state['caught_up_started_events']:
                # start up recurring events that depend on us being fully caught up with the blockchain to run
                CaughtUpProcessor.run_active_functions()

                config.state['caught_up_started_events'] = True

            publish_mempool_tx()
            time.sleep(2)  # counterblockd itself is at least caught up, wait a bit to query again for the latest block from cpd
