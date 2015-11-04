"""
track message types, for compiling of statistics
Implements transaction stats support as a counterblock plugin

Python 2.x, as counterblock is still python 2.x
"""
import os
import sys
import time
import datetime
import logging
import json
import ConfigParser

import pymongo
from bson.son import SON
import dateutil.parser

from counterblock.lib import config, util, blockfeed, blockchain
from counterblock.lib.processor import MessageProcessor, MempoolMessageProcessor, BlockProcessor, StartUpProcessor, CaughtUpProcessor, RollbackProcessor, API, start_task, CORE_FIRST_PRIORITY

logger = logging.getLogger(__name__)

@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY-1) #this priority here is important
def parse_insert(msg, msg_data): 
    if msg['command'] == 'insert' \
       and msg['category'] not in ["debits", "credits", "order_matches", "bet_matches",
           "order_expirations", "bet_expirations", "order_match_expirations", "bet_match_expirations", "bet_match_resolutions"]:
        config.mongo_db.transaction_stats.insert({
            'block_index': config.state['cur_block']['block_index'],
            'block_time': config.state['cur_block']['block_time_obj'],
            'category': msg['category']
        })

@API.add_method
def get_transaction_stats(start_ts=None, end_ts=None):
    now_ts = time.mktime(datetime.datetime.utcnow().timetuple())
    if not end_ts: #default to current datetime
        end_ts = now_ts
    if not start_ts: #default to 360 days before the end date
        start_ts = end_ts - (360 * 24 * 60 * 60)
            
    stats = config.mongo_db.transaction_stats.aggregate([
        {"$match": {
            "block_time": {
                "$gte": datetime.datetime.utcfromtimestamp(start_ts)
            } if end_ts == now_ts else {
                "$gte": datetime.datetime.utcfromtimestamp(start_ts),
                "$lte": datetime.datetime.utcfromtimestamp(end_ts)                    
            }                
        }},
        {"$project": {
            "year":  {"$year": "$block_time"},
            "month": {"$month": "$block_time"},
            "day":   {"$dayOfMonth": "$block_time"},
            "category": 1,
        }},
        {"$group": {
            "_id":   {"year": "$year", "month": "$month", "day": "$day", "category": "$category"},
            "count": {"$sum": 1},
        }}
        #{"$sort": SON([("_id.year", pymongo.ASCENDING), ("_id.month", pymongo.ASCENDING), ("_id.day", pymongo.ASCENDING), ("_id.hour", pymongo.ASCENDING), ("_id.category", pymongo.ASCENDING)])},
    ])
    times = {}
    categories = {}
    stats = [] if not stats['ok'] else stats['result']
    for e in stats:
        categories.setdefault(e['_id']['category'], {})
        time_val = int(time.mktime(datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']).timetuple()) * 1000)
        times.setdefault(time_val, True)
        categories[e['_id']['category']][time_val] = e['count']
    times_list = times.keys()
    times_list.sort()
    #fill in each array with all found timestamps
    for e in categories:
        a = []
        for t in times_list:
            a.append([t, categories[e][t] if t in categories[e] else 0])
        categories[e] = a #replace with array data
    #take out to final data structure
    categories_list = []
    for k, v in categories.iteritems():
        categories_list.append({'name': k, 'data': v})
    return categories_list

@StartUpProcessor.subscribe()
def init():
    #init db and indexes
    #transaction_stats
    config.mongo_db.transaction_stats.ensure_index([ #blockfeed.py, api.py
        ("when", pymongo.ASCENDING),
        ("category", pymongo.DESCENDING)
    ])
    config.mongo_db.transaction_stats.ensure_index('block_index')

@CaughtUpProcessor.subscribe()
def start_tasks(): 
    pass

@RollbackProcessor.subscribe()
def process_rollback(max_block_index):
    if not max_block_index: #full reparse
        config.mongo_db.transaction_stats.drop()
    else: #rollback
        config.mongo_db.transaction_stats.remove({"block_index": {"$gt": max_block_index}})
