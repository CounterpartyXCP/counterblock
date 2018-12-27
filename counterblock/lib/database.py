import os
import logging
import pymongo

from counterblock.lib import config, cache, util
from counterblock.lib.processor import RollbackProcessor

logger = logging.getLogger(__name__)


def get_connection():
    """Connect to mongodb, returning a connection object"""
    logger.info("Connecting to mongoDB backend ...")
    mongo_client = pymongo.MongoClient(config.MONGODB_CONNECT, config.MONGODB_PORT)
    mongo_db = mongo_client[config.MONGODB_DATABASE]  # will create if it doesn't exist
    if config.MONGODB_USER and config.MONGODB_PASSWORD:
        if not mongo_db.authenticate(config.MONGODB_USER, config.MONGODB_PASSWORD):
            raise Exception("Could not authenticate to mongodb with the supplied username and password.")
    return mongo_db


def init_base_indexes():
    """insert mongo indexes if need-be (i.e. for newly created database)"""
    # COLLECTIONS THAT ARE PURGED AS A RESULT OF A REPARSE
    # processed_blocks
    config.mongo_db.processed_blocks.ensure_index('block_index', unique=True)
    # COLLECTIONS THAT ARE *NOT* PURGED AS A RESULT OF A REPARSE
    # mempool
    config.mongo_db.mempool.ensure_index('tx_hash')


def get_block_indexes_for_dates(start_dt=None, end_dt=None):
    """Returns a 2 tuple (start_block, end_block) result for the block range that encompasses the given start_date
    and end_date unix timestamps"""
    if start_dt is None:
        start_block_index = config.BLOCK_FIRST
    else:
        start_block = config.mongo_db.processed_blocks.find_one({"block_time": {"$lte": start_dt}}, sort=[("block_time", pymongo.DESCENDING)])
        start_block_index = config.BLOCK_FIRST if not start_block else start_block['block_index']

    if end_dt is None:
        end_block_index = config.state['my_latest_block']['block_index']
    else:
        end_block = config.mongo_db.processed_blocks.find_one({"block_time": {"$gte": end_dt}}, sort=[("block_time", pymongo.ASCENDING)])
        if not end_block:
            end_block_index = config.mongo_db.processed_blocks.find_one(sort=[("block_index", pymongo.DESCENDING)])['block_index']
        else:
            end_block_index = end_block['block_index']
    return (start_block_index, end_block_index)


def get_block_time(block_index):
    """TODO: implement result caching to avoid having to go out to the database"""
    block = config.mongo_db.processed_blocks.find_one({"block_index": block_index})
    if not block:
        return None
    return block['block_time']


def reset_db_state():
    """boom! blow away all applicable collections in mongo"""
    config.mongo_db.processed_blocks.drop()

    # create/update default app_config object
    config.mongo_db.app_config.update({}, {
        'db_version': config.DB_VERSION,  # counterblockd database version
        'running_testnet': config.TESTNET,
        'running_regtest': config.REGTEST,
        'counterpartyd_db_version_major': None,
        'counterpartyd_db_version_minor': None,
        'counterpartyd_running_testnet': None,
        'counterpartyd_running_regtest': None,
        'last_block_assets_compiled': config.BLOCK_FIRST,  # for asset data compilation in tasks.py (resets on reparse as well)
    }, upsert=True)
    app_config = config.mongo_db.app_config.find()[0]

    # reinitialize some internal counters
    config.state['my_latest_block'] = {'block_index': 0}
    config.state['last_message_index'] = -1

    # call any rollback processors for any extension modules
    RollbackProcessor.run_active_functions(None)

    return app_config


def init_reparse(quit_after=False):
    app_config = reset_db_state()
    config.state['my_latest_block'] = config.LATEST_BLOCK_INIT

    config.IS_REPARSING = True
    if quit_after:
        config.QUIT_AFTER_CAUGHT_UP = True

    return app_config


def rollback(max_block_index):
    """called if there are any records for blocks higher than this in the database? If so, they were impartially created
       and we should get rid of them

    NOTE: after calling this function, you should always trigger a "continue" statement to reiterate the processing loop
    (which will get a new cp_latest_block from counterpartyd and resume as appropriate)
    """
    assert isinstance(max_block_index, int) and max_block_index >= config.BLOCK_FIRST
    if not config.mongo_db.processed_blocks.find_one({"block_index": max_block_index}):
        raise Exception("Can't roll back to specified block index: %i doesn't exist in database" % max_block_index)

    logger.warn("Pruning to block %i ..." % (max_block_index))
    config.mongo_db.processed_blocks.remove({"block_index": {"$gt": max_block_index}})

    config.state['last_message_index'] = -1
    config.state['caught_up'] = False
    cache.clear_block_info_cache()
    config.state['my_latest_block'] = config.mongo_db.processed_blocks.find_one({"block_index": max_block_index}) or config.LATEST_BLOCK_INIT

    # call any rollback processors for any extension modules
    RollbackProcessor.run_active_functions(max_block_index)
