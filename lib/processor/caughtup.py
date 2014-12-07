from lib import config, events
import gevent
import logging
from processor import CaughtUpProcessor
@CaughtUpProcessor.subscribe(priority=69)
def spawn_compile_asset_pair_market_info():
    logging.debug("Starting event timer: compile_asset_pair_market_info")
    gevent.spawn(events.compile_asset_pair_market_info)
@CaughtUpProcessor.subscribe(priority=68)
def spawn_compile_asset_market_info(): 
    logging.debug("Starting event timer: compile_asset_market_info")
    gevent.spawn(events.compile_asset_market_info)
@CaughtUpProcessor.subscribe(priority=67)
def spawn_compile_extended_asset_info(): 
    logging.debug("Starting event timer: compile_extended_asset_info")
    gevent.spawn(events.compile_extended_asset_info)
@CaughtUpProcessor.subscribe(priority=66)
def spawn_compile_extended_feed_info():
    logging.debug("Starting event timer: compile_extended_feed_info")
    gevent.spawn(events.compile_extended_feed_info)
