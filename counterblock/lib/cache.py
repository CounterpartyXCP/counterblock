import os
import hashlib
import logging
import json
import gevent
import redis
import redis.connection
redis.connection.socket = gevent.socket  # make redis play well with gevent

from counterblock.lib import config, util

DEFAULT_REDIS_CACHE_PERIOD = 60  # in seconds

logger = logging.getLogger(__name__)
blockinfo_cache = {}

##
# REDIS-RELATED
##
def get_redis_connection():
    logger.info("Connecting to redis @ %s" % config.REDIS_CONNECT)
    return redis.StrictRedis(host=config.REDIS_CONNECT, port=config.REDIS_PORT, db=config.REDIS_DATABASE)


def get_value(key):
    if not config.REDIS_CLIENT:
        logger.debug("Cache MISS: {}".format(key))
        return None
    result = config.REDIS_CLIENT.get(key)
    logger.debug("Cache {}: {}".format('HIT' if result is not None else 'MISS', key))
    return json.loads(result.decode('utf8')) if result is not None else result


def set_value(key, value, cache_period=DEFAULT_REDIS_CACHE_PERIOD):
    logger.debug("Caching key {} -- period: {}".format(key, cache_period))
    if not config.REDIS_CLIENT:
        return
    config.REDIS_CLIENT.setex(key, cache_period, json.dumps(value))


##
# NOT REDIS RELATED
##
def get_block_info(block_index, prefetch=0, min_message_index=None):
    global blockinfo_cache
    if block_index in blockinfo_cache:
        return blockinfo_cache[block_index]

    blockinfo_cache.clear()
    blocks = util.call_jsonrpc_api(
        'get_blocks',
        {'block_indexes': list(range(block_index, block_index + prefetch)),
         'min_message_index': min_message_index},
        abort_on_error=True)['result']
    for block in blocks:
        blockinfo_cache[block['block_index']] = block
    return blockinfo_cache[block_index]
