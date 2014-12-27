import os
import re
import logging
import datetime
import time
import copy
import decimal
import json
import urllib
import StringIO

import pymongo
import gevent
from PIL import Image

from lib import config, util, blockchain
from lib.components import assets, assets_trading, betting

D = decimal.Decimal
COMPILE_MARKET_PAIR_INFO_PERIOD = 10 * 60 #in seconds (this is every 10 minutes currently)
COMPILE_ASSET_MARKET_INFO_PERIOD = 30 * 60 #in seconds (this is every 30 minutes currently)

def check_blockchain_service():
    try:
        blockchain.check()
    except Exception as e:
        raise Exception('Could not connect to blockchain service: %s' % e)
    finally:
        gevent.spawn_later(5 * 60, check_blockchain_service) #call again in 5 minutes

def expire_stale_prefs():
    """
    Every day, clear out preferences objects that haven't been touched in > 30 days, in order to reduce abuse risk/space consumed
    """
    mongo_db = config.mongo_db
    min_last_updated = time.mktime((datetime.datetime.utcnow() - datetime.timedelta(days=30)).timetuple())
    
    num_stale_records = config.mongo_db.preferences.find({'last_touched': {'$lt': min_last_updated}}).count()
    mongo_db.preferences.remove({'last_touched': {'$lt': min_last_updated}})
    if num_stale_records: logging.warn("REMOVED %i stale preferences objects" % num_stale_records)
    
    #call again in 1 day
    gevent.spawn_later(86400, expire_stale_prefs)

def expire_stale_btc_open_order_records():
    mongo_db = config.mongo_db
    min_when_created = time.mktime((datetime.datetime.utcnow() - datetime.timedelta(days=15)).timetuple())
    
    num_stale_records = config.mongo_db.btc_open_orders.find({'when_created': {'$lt': min_when_created}}).count()
    mongo_db.btc_open_orders.remove({'when_created': {'$lt': min_when_created}})
    if num_stale_records: logging.warn("REMOVED %i stale BTC open order objects" % num_stale_records)
    
    #call again in 1 day
    gevent.spawn_later(86400, expire_stale_btc_open_order_records)
    
def generate_wallet_stats():
    """
    Every 30 minutes, from the login history, update and generate wallet stats
    """
    mongo_db = config.mongo_db
    
    def gen_stats_for_network(network):
        assert network in ('mainnet', 'testnet')
        #get the latest date in the stats table present
        now = datetime.datetime.utcnow()
        latest_stat = mongo_db.wallet_stats.find({'network': network}).sort('when', pymongo.DESCENDING).limit(1)
        latest_stat = latest_stat[0] if latest_stat.count() else None
        new_entries = {}
        
        #the queries below work with data that happened on or after the date of the latest stat present
        #aggregate over the same period for new logins, adding the referrers to a set
        match_criteria = {'when': {"$gte": latest_stat['when']}, 'network': network, 'action': 'create'} \
            if latest_stat else {'when': {"$lte": now}, 'network': network, 'action': 'create'}
        new_wallets = mongo_db.login_history.aggregate([
            {"$match": match_criteria },
            {"$project": {
                "year":  {"$year": "$when"},
                "month": {"$month": "$when"},
                "day":   {"$dayOfMonth": "$when"}
            }},
            {"$group": {
                "_id":   {"year": "$year", "month": "$month", "day": "$day"},
                "new_count": {"$sum": 1}
            }}
        ])
        new_wallets = [] if not new_wallets['ok'] else new_wallets['result']
        for e in new_wallets:
            ts = time.mktime(datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']).timetuple())
            new_entries[ts] = { #a future wallet_stats entry
                'when': datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']),
                'network': network,
                'new_count': e['new_count'],
            }
    
        referer_counts = mongo_db.login_history.aggregate([
            {"$match": match_criteria },
            {"$project": {
                "year":  {"$year": "$when"},
                "month": {"$month": "$when"},
                "day":   {"$dayOfMonth": "$when"},
                "referer": 1
            }},
            {"$group": {
                "_id":   {"year": "$year", "month": "$month", "day": "$day", "referer": "$referer"},
                #"uniqueReferers": {"$addToSet": "$_id"},
                "count": {"$sum": 1}
            }}
        ])
        referer_counts = [] if not referer_counts['ok'] else referer_counts['result']
        for e in referer_counts:
            ts = time.mktime(datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']).timetuple())
            assert ts in new_entries
            referer_key = urllib.quote(e['_id']['referer']).replace('.', '%2E')
            if 'referers' not in new_entries[ts]: new_entries[ts]['referers'] = {}
            if e['_id']['referer'] not in new_entries[ts]['referers']: new_entries[ts]['referers'][referer_key] = 0
            new_entries[ts]['referers'][referer_key] += 1
    
        #logins (not new wallets) - generate stats
        match_criteria = {'when': {"$gte": latest_stat['when']}, 'network': network, 'action': 'login'} \
            if latest_stat else {'when': {"$lte": now}, 'network': network, 'action': 'login'}
        logins = mongo_db.login_history.aggregate([
            {"$match": match_criteria },
            {"$project": {
                "year":  {"$year": "$when"},
                "month": {"$month": "$when"},
                "day":   {"$dayOfMonth": "$when"},
                "wallet_id": 1
            }},
            {"$group": {
                "_id":   {"year": "$year", "month": "$month", "day": "$day"},
                "login_count":   {"$sum": 1},
                "distinct_wallets":   {"$addToSet": "$wallet_id"},
            }}
        ])
        logins = [] if not logins['ok'] else logins['result']
        for e in logins:
            ts = time.mktime(datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']).timetuple())
            if ts not in new_entries:
                new_entries[ts] = { #a future wallet_stats entry
                    'when': datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']),
                    'network': network,
                    'new_count': 0,
                    'referers': []
                }
            new_entries[ts]['login_count'] = e['login_count']
            new_entries[ts]['distinct_login_count'] = len(e['distinct_wallets'])
            
        #add/replace the wallet_stats data
        if latest_stat:
            updated_entry_ts = time.mktime(datetime.datetime(
                latest_stat['when'].year, latest_stat['when'].month, latest_stat['when'].day).timetuple())
            if updated_entry_ts in new_entries:
                updated_entry = new_entries[updated_entry_ts]
                del new_entries[updated_entry_ts]
                assert updated_entry['when'] == latest_stat['when']
                del updated_entry['when'] #not required for the upsert
                logging.info("Revised wallet statistics for partial day %s-%s-%s: %s" % (
                    latest_stat['when'].year, latest_stat['when'].month, latest_stat['when'].day, updated_entry))
                mongo_db.wallet_stats.update({'when': latest_stat['when']},
                    {"$set": updated_entry}, upsert=True)
        
        if new_entries: #insert the rest
            #logging.info("Stats, new entries: %s" % new_entries.values())
            mongo_db.wallet_stats.insert(new_entries.values())
            logging.info("Added wallet statistics for %i full days" % len(new_entries.values()))
        
    gen_stats_for_network('mainnet')
    gen_stats_for_network('testnet')
    #call again in 30 minutes
    gevent.spawn_later(30 * 60, generate_wallet_stats)

def compile_asset_pair_market_info():
    assets_trading.compile_asset_pair_market_info()
    #all done for this run...call again in a bit                            
    gevent.spawn_later(COMPILE_MARKET_PAIR_INFO_PERIOD, compile_asset_pair_market_info)

def compile_extended_asset_info():
    assets.fetch_all_asset_info(config.mongo_db)
    #call again in 60 minutes
    gevent.spawn_later(60 * 60, compile_extended_asset_info)

def compile_extended_feed_info():
    betting.fetch_all_feed_info(config.mongo_db)
    #call again in 5 minutes
    gevent.spawn_later(60 * 5, compile_extended_feed_info)

def compile_asset_market_info():
    assets_trading.compile_asset_market_info()
    #all done for this run...call again in a bit                            
    gevent.spawn_later(COMPILE_ASSET_MARKET_INFO_PERIOD, compile_asset_market_info)
    
