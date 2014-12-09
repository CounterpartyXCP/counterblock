import time
import logging
import sys
import os
from lib import config
from lib.processor import processor
import pymongo
import subprocess
import json
import hashlib
dbhash_file = os.path.join(config.DATA_DIR, "dbhashes.txt")
standard_collections = [u'asset_pair_market_info', u'tracked_assets', u'balance_changes', u'transaction_stats', u'trades', u'processed_blocks']

def get_md5_collection_hashes(): 
    db_hashes = {} 
    for col_name in standard_collections: 
        db_hashes[col_name] = hashlib.md5(str(list(config.mongo_db[col_name].find({}, {"_id": 0})))).hexdigest()
    return db_hashes
    
def get_db_hash():
    return config.mongo_db.command('dbHash')
    
@processor.StartUpProcessor.subscribe()
def reparse_timer_start(): 
    config.BLOCK_FIRST = 281000
    config.REPARSE_FORCED = True
    config.state['timer'] = time.time()
    logging.info("Started reparse timer")

@processor.CaughtUpProcessor.subscribe(priority=90)
def reparse_timer_stop(): 
    msg = "Caught up To Blockchain" if config.CAUGHT_UP else "Stopped at %i, Counterpartyd is at %i" %(config.state['my_latest_block']['block_index'], config.state['last_processed_block']['block_index'])
    logging.warn("%s, time elapsed %s" %(msg, time.time() - config.state['timer']))
    sys.exit(1)

@processor.BlockProcessor.subscribe()
def stop_counterblockd(): 
    if config.state['my_latest_block']['block_index'] == 313553: 
        logging.warn("exitting at 31300 ...")
        log_database_hashes() 
        reparse_timer_stop()
        
def log_database_hashes(): 
    cur_hash = get_md5_collection_hashes()
    #Sometimes this method gives errors
    #head_label = subprocess.check_output(["git", "git rev-parse --verify HEAD"])
    proc = subprocess.Popen("git rev-parse --verify HEAD", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    head_label = proc.stdout.read().decode().strip('\n')
    infoErr = proc.stderr.read().decode()
    if infoErr: raise Exception(infoErr)
    db_info = get_db_info_from_file()
    db_info[head_label] = cur_hash
    logging.info("storing db hashes to file for Head %s" %head_label)
    with open(os.path.join(config.DATA_DIR, "dbhashes.txt"), 'w') as wfile: 
        json.dump(db_info, wfile)
        
def get_db_info_from_file():
    global dbhash_file 
    try:
        if os.stat(dbhash_file).st_size == 0: raise Exception
        else:
            with open(dbhash_file, 'r') as rfile: 
                return json.load(rfile)
    except: return {}

def compare_md5_database_hashes(): 
    global dbhash_file
    db_info = get_db_info_from_file()
    while len(db_info) > 1:
        head_label, db_hash = db_info.popitem()
        for other_label, other_hash in db_info.items():
            print("Comparing DB hashes for Git Heads: %s And %s" %(head_label, other_label))
            for col, col_hash in db_hash.items():
                if not other_hash.get(col): 
                    print("Collection does not exist %s in %s skipping..." %(col, other_label))
                    continue 
                try: 
                    assert(col_hash == other_hash[col])
                    msg = "OK..."
                except: 
                    msg = "Failed..."
                print("Comparing Collections %s, %s == %s   %s" %(col, col_hash, other_hash[col], msg))

def compare_default_database_hashes():
    global dbhash_file 
    db_info = get_db_info_from_file()
    while len(db_info) > 1:
        head_label, db_hash = db_info.popitem()
        for other_label, other_hash in db_info.items():
            print("Comparing DB hashes for Git Heads: %s And %s" %(head_label, other_label))
            for i, j in db_hash.items():
                if i == 'collections': 
                    for col, col_hash in j.items(): 
                        if not other_hash[i].get(col): 
                            print("Collection does not exist %s in %s skipping..." %(col, other_label))
                            continue 
                        try: 
                            assert(col_hash == other_hash[i][col])
                            msg = "OK..."
                        except: 
                            msg = "Failed..."
                        print("Comparing Collections %s, %s == %s   %s" %(col, col_hash, other_hash[i][col], msg))
                else:
                    try: 
                        assert(j == other_hash[i])
                        msg = "OK..."
                    except: 
                        msg = "Failed..."
                    print("Comparing %s, %s == %s   %s" %(i, j, other_hash[i], msg))

if __name__ == '__maine__':
    comparse_md5_database_hashes()
    
    
            
    
        

