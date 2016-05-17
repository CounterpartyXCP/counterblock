import os
import hashlib
import logging
import json
import gevent
import redis
import redis.connection
redis.connection.socket = gevent.socket #make redis play well with gevent

from counterblock.lib import config, util

logger = logging.getLogger(__name__)
blockinfo_cache = {} 

##
## REDIS-RELATED
##
def get_redis_connection():
    logger.info("Connecting to redis @ %s" % config.REDIS_CONNECT)
    return redis.StrictRedis(host=config.REDIS_CONNECT, port=config.REDIS_PORT, db=config.REDIS_DATABASE)

##
## NOT REDIS RELATED
##
def get_block_info(block_index, prefetch=0, min_message_index=None):
    global blockinfo_cache
    if block_index in blockinfo_cache:
        return blockinfo_cache[block_index]
    
    blockinfo_cache.clear()
    blocks = util.call_jsonrpc_api('get_blocks',
                              {'block_indexes': list(range(block_index, block_index + prefetch)),
                               'min_message_index': min_message_index},
                              abort_on_error=True)['result']
    for block in blocks:
        blockinfo_cache[block['block_index']] = block
    return blockinfo_cache[block_index]

def block_cache(func):
    """decorator"""
    def cached_function(*args, **kwargs):
        sql = "SELECT block_index FROM blocks ORDER BY block_index DESC LIMIT 1"
        block_index = util.call_jsonrpc_api('sql', {'query': sql, 'bindings': []})['result'][0]['block_index']
        function_signature = hashlib.sha256(func.__name__ + str(args) + str(kwargs)).hexdigest()

        cached_result = config.mongo_db.counterblockd_cache.find_one({'block_index': block_index, 'function': function_signature})

        if not cached_result or config.TESTNET:
            #logger.info("generate cache ({}, {}, {})".format(func.__name__, block_index, function_signature))
            try:
                result = func(*args, **kwargs)
                config.mongo_db.counterblockd_cache.insert({
                    'block_index': block_index, 
                    'function': function_signature,
                    'result': json.dumps(result)
                })
                return result
            except Exception as e:
                logger.exception(e)
        else:
            #logger.info("result from cache ({}, {}, {})".format(func.__name__, block_index, function_signature))
            result = json.loads(cached_result['result'])
            return result
            
    return cached_function

def clean_block_cache(block_index):
    #logger.info("clean block cache lower than {}".format(block_index))
    config.mongo_db.counterblockd_cache.remove({'block_index': {'$lt': block_index}})
