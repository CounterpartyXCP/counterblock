import gevent
import logging
import time

from lib import config
from . import CaughtUpProcessor, CORE_FIRST_PRIORITY, CORE_LAST_PRIORITY, tasks

logger = logging.getLogger(__name__)

@CaughtUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 0)
def spawn_compile_asset_pair_market_info():
    logger.debug("Starting event timer: compile_asset_pair_market_info")
    gevent.spawn(tasks.compile_asset_pair_market_info)
    
@CaughtUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 1)
def spawn_compile_asset_market_info(): 
    logger.debug("Starting event timer: compile_asset_market_info")
    gevent.spawn(tasks.compile_asset_market_info)
    
@CaughtUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 2)
def spawn_compile_extended_asset_info(): 
    logger.debug("Starting event timer: compile_extended_asset_info")
    gevent.spawn(tasks.compile_extended_asset_info)
    
@CaughtUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 3)
def spawn_compile_extended_feed_info():
    logger.debug("Starting event timer: compile_extended_feed_info")
    gevent.spawn(tasks.compile_extended_feed_info)
