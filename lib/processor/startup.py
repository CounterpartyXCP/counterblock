import logging
import gevent
from lib import blockfeed, events, api, config
from processor import StartUpProcessor

@StartUpProcessor.subscribe(priority=70)
def start_cpd_blockfeed():
    logging.info("Starting up counterpartyd block feed poller...")
    gevent.spawn(blockfeed.process_cpd_blockfeed, config.ZMQ_PUBLISHER_EVENTFEED)
@StartUpProcessor.subscribe(priority=69)
def check_blockchain_service():
    logging.debug("Starting event timer: check_blockchain_service")
    gevent.spawn(events.check_blockchain_service)
@StartUpProcessor.subscribe(priority=68)
def expire_stale_prefs():
    logging.debug("Starting event timer: expire_stale_prefs")
    gevent.spawn(events.expire_stale_prefs)
@StartUpProcessor.subscribe(priority=67)
def expire_stale_orders():
    logging.debug("Starting event timer: expire_stale_btc_open_order_records")
    gevent.spawn(events.expire_stale_btc_open_order_records)
@StartUpProcessor.subscribe(priority=66)
def generate_wallet_stats():
    logging.debug("Starting event timer: generate_wallet_stats")
    gevent.spawn(events.generate_wallet_stats)
@StartUpProcessor.subscribe(priority=-1)
def start_api():
    logging.info("Starting up RPC API handler...")
    api.serve_api(config.mongo_db, config.REDIS_CLIENT)

