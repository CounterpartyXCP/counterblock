import os
import logging
import pymongo

from lib import config, util

def get_connection():
    """Connect to mongodb, returning a connection object"""
    logging.info("Connecting to mongoDB backend ...")
    mongo_client = pymongo.MongoClient(config.MONGODB_CONNECT, config.MONGODB_PORT)
    mongo_db = mongo_client[config.MONGODB_DATABASE] #will create if it doesn't exist
    if config.MONGODB_USER and config.MONGODB_PASSWORD:
        if not mongo_db.authenticate(config.MONGODB_USER, config.MONGODB_PASSWORD):
            raise Exception("Could not authenticate to mongodb with the supplied username and password.")
    return mongo_db

def init_base_indexes(mongo_db):
    """insert mongo indexes if need-be (i.e. for newly created database)"""
    ##COLLECTIONS THAT ARE PURGED AS A RESULT OF A REPARSE
    #processed_blocks
    mongo_db.processed_blocks.ensure_index('block_index', unique=True)
    #tracked_assets
    mongo_db.tracked_assets.ensure_index('asset', unique=True)
    mongo_db.tracked_assets.ensure_index('_at_block') #for tracked asset pruning
    mongo_db.tracked_assets.ensure_index([
        ("owner", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
    ])
    #trades
    mongo_db.trades.ensure_index([
        ("base_asset", pymongo.ASCENDING),
        ("quote_asset", pymongo.ASCENDING),
        ("block_time", pymongo.DESCENDING)
    ])
    mongo_db.trades.ensure_index([ #tasks.py and elsewhere (for singlular block_index index access)
        ("block_index", pymongo.ASCENDING),
        ("base_asset", pymongo.ASCENDING),
        ("quote_asset", pymongo.ASCENDING)
    ])

    #balance_changes
    mongo_db.balance_changes.ensure_index('block_index')
    mongo_db.balance_changes.ensure_index([
        ("address", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
        ("block_time", pymongo.ASCENDING)
    ])
    #asset_market_info
    mongo_db.asset_market_info.ensure_index('asset', unique=True)
    #asset_marketcap_history
    mongo_db.asset_marketcap_history.ensure_index('block_index')
    mongo_db.asset_marketcap_history.ensure_index([ #tasks.py
        ("market_cap_as", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
        ("block_index", pymongo.DESCENDING)
    ])
    mongo_db.asset_marketcap_history.ensure_index([ #api.py
        ("market_cap_as", pymongo.ASCENDING),
        ("block_time", pymongo.DESCENDING)
    ])
    #asset_pair_market_info
    mongo_db.asset_pair_market_info.ensure_index([ #event.py, api.py
        ("base_asset", pymongo.ASCENDING),
        ("quote_asset", pymongo.ASCENDING)
    ], unique=True)
    mongo_db.asset_pair_market_info.ensure_index('last_updated')
    #asset_extended_info
    mongo_db.asset_extended_info.ensure_index('asset', unique=True)
    mongo_db.asset_extended_info.ensure_index('info_status')
    #btc_open_orders
    mongo_db.btc_open_orders.ensure_index('when_created')
    mongo_db.btc_open_orders.ensure_index('order_tx_hash', unique=True)
    #transaction_stats
    mongo_db.transaction_stats.ensure_index([ #blockfeed.py, api.py
        ("when", pymongo.ASCENDING),
        ("category", pymongo.DESCENDING)
    ])
    mongo_db.transaction_stats.ensure_index('message_index', unique=True)
    mongo_db.transaction_stats.ensure_index('block_index')
    #wallet_stats
    mongo_db.wallet_stats.ensure_index([
        ("when", pymongo.ASCENDING),
        ("network", pymongo.ASCENDING),
    ])
    
    ##COLLECTIONS THAT ARE *NOT* PURGED AS A RESULT OF A REPARSE
    #preferences
    mongo_db.preferences.ensure_index('wallet_id', unique=True)
    mongo_db.preferences.ensure_index('network')
    mongo_db.preferences.ensure_index('last_touched')
    #login_history
    mongo_db.login_history.ensure_index('wallet_id')
    mongo_db.login_history.ensure_index([
        ("when", pymongo.DESCENDING),
        ("network", pymongo.ASCENDING),
        ("action", pymongo.ASCENDING),
    ])
    #chat_handles
    mongo_db.chat_handles.ensure_index('wallet_id', unique=True)
    mongo_db.chat_handles.ensure_index('handle', unique=True)
    #chat_history
    mongo_db.chat_history.ensure_index('when')
    mongo_db.chat_history.ensure_index([
        ("handle", pymongo.ASCENDING),
        ("when", pymongo.DESCENDING),
    ])
    #feeds
    mongo_db.feeds.ensure_index('source')
    mongo_db.feeds.ensure_index('owner')
    mongo_db.feeds.ensure_index('category')
    mongo_db.feeds.ensure_index('info_url')
    #mempool
    mongo_db.mempool.ensure_index('tx_hash')

def get_block_indexes_for_dates(start_dt=None, end_dt=None):
    """Returns a 2 tuple (start_block, end_block) result for the block range that encompasses the given start_date
    and end_date unix timestamps"""
    if start_dt is None:
        start_block_index = config.BLOCK_FIRST
    else:
        start_block = config.mongo_db.processed_blocks.find_one({"block_time": {"$lte": start_dt} }, sort=[("block_time", pymongo.DESCENDING)])
        start_block_index = config.BLOCK_FIRST if not start_block else start_block['block_index']
    
    if end_dt is None:
        end_block_index = config.state['my_latest_block']['block_index']
    else:
        end_block = config.mongo_db.processed_blocks.find_one({"block_time": {"$gte": end_dt} }, sort=[("block_time", pymongo.ASCENDING)])
        if not end_block:
            end_block_index = config.mongo_db.processed_blocks.find_one(sort=[("block_index", pymongo.DESCENDING)])['block_index']
        else:
            end_block_index = end_block['block_index']
    return (start_block_index, end_block_index)

def get_block_time(block_index):
    """TODO: implement result caching to avoid having to go out to the database"""
    block = config.mongo_db.processed_blocks.find_one({"block_index": block_index })
    if not block: return None
    return block['block_time']

def reset_db_state():
    """boom! blow away all applicable collections in mongo"""
    config.mongo_db.processed_blocks.drop()
    config.mongo_db.tracked_assets.drop()
    config.mongo_db.trades.drop()
    config.mongo_db.balance_changes.drop()
    config.mongo_db.asset_market_info.drop()
    config.mongo_db.asset_marketcap_history.drop()
    config.mongo_db.pair_market_info.drop()
    config.mongo_db.btc_open_orders.drop()
    config.mongo_db.asset_extended_info.drop()
    config.mongo_db.transaction_stats.drop()
    config.mongo_db.feeds.drop()
    config.mongo_db.wallet_stats.drop()
    
    #create/update default app_config object
    config.mongo_db.app_config.update({}, {
    'db_version': config.DB_VERSION, #counterblockd database version
    'running_testnet': config.TESTNET,
    'counterpartyd_db_version_major': None,
    'counterpartyd_db_version_minor': None,
    'counterpartyd_running_testnet': None,
    'last_block_assets_compiled': config.BLOCK_FIRST, #for asset data compilation in tasks.py (resets on reparse as well)
    }, upsert=True)
    app_config = config.mongo_db.app_config.find()[0]
    
    #DO NOT DELETE preferences and chat_handles and chat_history
    
    #create XCP and BTC assets in tracked_assets
    for asset in [config.XCP, config.BTC]:
        base_asset = {
            'asset': asset,
            'owner': None,
            'divisible': True,
            'locked': False,
            'total_issued': None,
            '_at_block': config.BLOCK_FIRST, #the block ID this asset is current for
            '_history': [] #to allow for block rollbacks
        }
        config.mongo_db.tracked_assets.insert(base_asset)
        
    #reinitialize some internal counters
    config.state['my_latest_block'] = {'block_index': 0}
    config.state['last_message_index'] = -1
    
    return app_config

