import os
import sys
import json
import time
import logging
import gevent

from lib import blockfeed, config, cache, siofeeds, database
from . import StartUpProcessor, CORE_FIRST_PRIORITY, CORE_LAST_PRIORITY, api, tasks

@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 0)
def load_counterwallet_config_settings():
    #TODO: Hardcode in cw path for now. Will be taken out to a plugin shortly...
    counterwallet_config_path = os.path.join('/home/xcp/counterwallet/counterwallet.conf.json')
    if os.path.exists(counterwallet_config_path):
        logging.info("Loading counterwallet config at '%s'" % counterwallet_config_path)
        with open(counterwallet_config_path) as f:
            config.COUNTERWALLET_CONFIG_JSON = f.read()
    else:
        logging.warn("Counterwallet config does not exist at '%s'. Counterwallet functionality disabled..." % counterwallet_config_path)
        config.COUNTERWALLET_CONFIG_JSON = '{}'
    try:
        config.COUNTERWALLET_CONFIG = json.loads(config.COUNTERWALLET_CONFIG_JSON)
    except Exception, e:
        logging.error("Exception loading counterwallet config: %s" % e)
        
@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 2)
def init_geoip():
    import pygeoip
    
    def download_geoip_data():
        logging.info("Checking/updating GeoIP.dat ...")
    
        download = False;
        data_path = os.path.join(config.DATA_DIR, 'GeoIP.dat')
        if not os.path.isfile(data_path):
            download = True
        else:
            one_week_ago = time.time() - 60*60*24*7
            file_stat = os.stat(data_path)
            if file_stat.st_ctime < one_week_ago:
                download = True
    
        if download:
            logging.info("Downloading GeoIP.dat")
            ##TODO: replace with pythonic way to do this!
            cmd = "cd '{}'; wget -N -q http://geolite.maxmind.com/download/geoip/database/GeoLiteCountry/GeoIP.dat.gz; gzip -dfq GeoIP.dat.gz".format(config.DATA_DIR)
            util.subprocess_cmd(cmd)
        else:
            logging.info("GeoIP.dat database up to date. Not downloading.")
    
    download_geoip_data()
    config.GEOIP =  pygeoip.GeoIP(os.path.join(config.DATA_DIR, 'GeoIP.dat'))

@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 3)
def init_mongo():
    config.mongo_db = database.get_connection() #should be able to access fine across greenlets, etc
    database.init_base_indexes(config.mongo_db)
    
@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 4)
def init_redis():
    config.REDIS_CLIENT = cache.get_redis_connection()
    
@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 5)
def init_siofeeds():
    siofeeds.set_up()

@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 6)
def start_cpd_blockfeed():
    logging.info("Starting up counterpartyd block feed poller...")
    gevent.spawn(blockfeed.process_cpd_blockfeed, config.ZMQ_PUBLISHER_EVENTFEED)
    
@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 7)
def check_blockchain_service():
    logging.debug("Starting event timer: check_blockchain_service")
    gevent.spawn(tasks.check_blockchain_service)
    
@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 8)
def expire_stale_prefs():
    logging.debug("Starting event timer: expire_stale_prefs")
    gevent.spawn(tasks.expire_stale_prefs)

@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 9)
def expire_stale_orders():
    logging.debug("Starting event timer: expire_stale_btc_open_order_records")
    gevent.spawn(tasks.expire_stale_btc_open_order_records)

@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 10)
def generate_wallet_stats():
    logging.debug("Starting event timer: generate_wallet_stats")
    gevent.spawn(tasks.generate_wallet_stats)
    
@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 11)
def warn_on_missing_support_email():
    if not config.SUPPORT_EMAIL:
        logging.warn("Support email setting not set: To enable, please specify an email for the 'support-email' setting in your counterblockd.conf")

@StartUpProcessor.subscribe(priority=CORE_LAST_PRIORITY - 0) #should go last (even after custom plugins)
def start_api():
    logging.info("Starting up RPC API handler...")
    api.serve_api()

