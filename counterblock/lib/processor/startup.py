import os
import sys
import json
import time
import logging

from counterblock.lib import blockfeed, blockchain, config, cache, database, util
from counterblock.lib.processor import StartUpProcessor, CORE_FIRST_PRIORITY, CORE_LAST_PRIORITY, api, start_task

logger = logging.getLogger(__name__)

@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 0)
def init_mongo():
    config.mongo_db = database.get_connection() #should be able to access fine across greenlets, etc
    database.init_base_indexes()
    
@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 1)
def init_redis():
    config.REDIS_CLIENT = cache.get_redis_connection()
    
@StartUpProcessor.subscribe(priority=CORE_LAST_PRIORITY - 0) #must come after all plugins have been initalized
def start_cp_blockfeed():
    logger.info("Starting up counterparty block feed poller...")
    start_task(blockfeed.process_cp_blockfeed)
    
@StartUpProcessor.subscribe(priority=CORE_LAST_PRIORITY - 1) #should go last (even after custom plugins)
def start_api():
    logger.info("Starting up RPC API handler...")
    group = start_task(api.serve_api)
    group.join() #block forever
