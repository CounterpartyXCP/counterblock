import os
import sys
import json
import time
import logging
import gevent
import gevent.pool
import gevent.util

from counterblock.lib import blockfeed, config, cache, siofeeds, database, util
from counterblock.lib.processor import StartUpProcessor, CORE_FIRST_PRIORITY, CORE_LAST_PRIORITY, api, tasks

logger = logging.getLogger(__name__)

class GreenletGroupWithExceptionCatching(gevent.pool.Group):
    """See https://gist.github.com/progrium/956006"""
    def __init__(self, *args):
        super(GreenletGroupWithExceptionCatching, self).__init__(*args)
        self._error_handlers = {}
    
    def _wrap_errors(self, func):
        """Wrap a callable for triggering error handlers
        
        This is used by the greenlet spawn methods so you can handle known
        exception cases instead of gevent's default behavior of just printing
        a stack trace for exceptions running in parallel greenlets.
        
        """
        def wrapped_f(*args, **kwargs):
            exceptions = tuple(self._error_handlers.keys())
            try:
                return func(*args, **kwargs)
            except exceptions, exception:
                for type in self._error_handlers:
                    if isinstance(exception, type):
                        handler, greenlet = self._error_handlers[type]
                        self._wrap_errors(handler)(exception, greenlet)
                return exception
        return wrapped_f
    
    def catch(self, type, handler):
        """Set an error handler for exceptions of `type` raised in greenlets"""
        self._error_handlers[type] = (handler, gevent.getcurrent())
    
    def spawn(self, func, *args, **kwargs):
        parent = super(GreenletGroupWithExceptionCatching, self)
        func_wrap = self._wrap_errors(func)
        return parent.spawn(func_wrap, *args, **kwargs)
    
    def spawn_later(self, seconds, func, *args, **kwargs):
        parent = super(GreenletGroupWithExceptionCatching, self)
        func_wrap = self._wrap_errors(func)
        return parent.spawn_later(seconds, func_wrap, *args, **kwargs)

def raise_in_handling_greenlet(error, greenlet):
    greenlet.throw(error)

@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 0)
def load_counterwallet_config_settings():
    #TODO: Hardcode in cw path for now. Will be taken out to a plugin shortly...
    counterwallet_config_path = os.path.join('/home/xcp/counterwallet/counterwallet.conf.json')
    if os.path.exists(counterwallet_config_path):
        logger.info("Loading counterwallet config at '%s'" % counterwallet_config_path)
        with open(counterwallet_config_path) as f:
            config.COUNTERWALLET_CONFIG_JSON = f.read()
    else:
        logger.warn("Counterwallet config does not exist at '%s'. Counterwallet functionality disabled..." % counterwallet_config_path)
        config.COUNTERWALLET_CONFIG_JSON = '{}'
    try:
        config.COUNTERWALLET_CONFIG = json.loads(config.COUNTERWALLET_CONFIG_JSON)
    except Exception, e:
        logger.error("Exception loading counterwallet config: %s" % e)
   
def on_init_exception(parent, e):
    #logger.exception(e)
    print "FOO", e, e.__traceback__
    tb = traceback.format_tb(e.__traceback__)
    logging.error( "TB: %s" % tb)
    gevent.kill(parent, e)
        
@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 2)
def init_geoip():
    import pygeoip
    
    def download_geoip_data():
        logger.info("Checking/updating GeoIP.dat ...")
    
        download = False;
        data_path = os.path.join(config.data_dir, 'GeoIP.dat')
        if not os.path.isfile(data_path):
            download = True
        else:
            one_week_ago = time.time() - 60*60*24*7
            file_stat = os.stat(data_path)
            if file_stat.st_ctime < one_week_ago:
                download = True
    
        if download:
            logger.info("Downloading GeoIP.dat")
            ##TODO: replace with pythonic way to do this!
            cmd = "cd '{}'; wget -N -q http://geolite.maxmind.com/download/geoip/database/GeoLiteCountry/GeoIP.dat.gz; gzip -dfq GeoIP.dat.gz".format(config.data_dir)
            util.subprocess_cmd(cmd)
        else:
            logger.info("GeoIP.dat database up to date. Not downloading.")
    
    download_geoip_data()
    config.GEOIP =  pygeoip.GeoIP(os.path.join(config.data_dir, 'GeoIP.dat'))

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
def start_cp_blockfeed():
    logger.info("Starting up counterparty block feed poller...")
    group = GreenletGroupWithExceptionCatching()
    group.catch(Exception, raise_in_handling_greenlet)
    group.spawn(blockfeed.process_cp_blockfeed, config.ZMQ_PUBLISHER_EVENTFEED)
        
@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 7)
def check_blockchain_service():
    logger.debug("Starting event timer: check_blockchain_service")
    group = GreenletGroupWithExceptionCatching()
    group.catch(Exception, raise_in_handling_greenlet)
    group.spawn(tasks.check_blockchain_service)
    
@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 8)
def expire_stale_prefs():
    logger.debug("Starting event timer: expire_stale_prefs")
    group = GreenletGroupWithExceptionCatching()
    group.catch(Exception, raise_in_handling_greenlet)
    group.spawn(tasks.expire_stale_prefs)

@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 9)
def expire_stale_orders():
    logger.debug("Starting event timer: expire_stale_btc_open_order_records")
    group = GreenletGroupWithExceptionCatching()
    group.catch(Exception, raise_in_handling_greenlet)
    group.spawn(tasks.expire_stale_btc_open_order_records)

@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 10)
def generate_wallet_stats():
    logger.debug("Starting event timer: generate_wallet_stats")
    group = GreenletGroupWithExceptionCatching()
    group.catch(Exception, raise_in_handling_greenlet)
    group.spawn(tasks.generate_wallet_stats)
    
@StartUpProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 11)
def warn_on_missing_support_email():
    if not config.SUPPORT_EMAIL:
        logger.warn("Support email setting not set: To enable, please specify an email for the 'support-email' setting in your counterblockd.conf")

@StartUpProcessor.subscribe(priority=CORE_LAST_PRIORITY - 0) #should go last (even after custom plugins)
def start_api():
    logger.info("Starting up RPC API handler...")
    group = GreenletGroupWithExceptionCatching()
    group.catch(Exception, raise_in_handling_greenlet)
    group.spawn(api.serve_api)
    group.join() #block forever
