from lib import config, util
from datetime import datetime
import sqlite3
import logging

class Betting:

    def __init__(self, mongo_db):
        self.db = mongo_db

    def parse_broadcast(self, message):
        logging.info('Parsing broadcast message..')
        logging.info(message)

        save = False
        feed = None
        if self.db.feeds:
            feed = self.db.feeds.find_one({"source": message['source']})
        if feed is None:
            feed = {'source': message['source']}
        elif message['locked'] and not feed['locked']:
            feed['locked'] = True
            save = True
        if util.is_valid_url(message['text'], suffix='.json') and message['value'] == -1.0:
            feed['info_url'] = message['text']
            feed['info_status'] = 'needfetch' #needfetch, valid (included in CW feed directory), invalid, error 
            feed['fetch_info_retry'] = 0 # we retry 3 times to fetch info from info_url
            feed['fee_fraction_int'] = message['fee_fraction_int']
            save = True
        if save:  
            logging.debug('Save feed: '+feed['info_url'])   
            self.db.feeds.save(feed)
            return True
        return False

    def inc_fetch_retry(self, feed, max_retry = 3, new_status = 'error', errors=[]):
        feed['fetch_info_retry'] += 1
        feed['errors'] = errors
        if feed['fetch_info_retry'] == max_retry:
            feed['info_status'] = new_status
        self.db.feeds.save(feed)

    def parse_deadlines(self, data):
        if 'deadlines' not in data: return False
        if not isinstance(data['deadlines'], list): return False
        if len(data['deadlines']) == 0: return False
        results = []
        for deadline in data['deadlines']:
            date = util.date_param(deadline)
            if date:
                results.append(date)
            else:
                return False
        return results

    def parse_outcomes(self, data):
        if 'outcomes' not in data: return False
        if not isinstance(data['outcomes'], list): return False
        if len(data['outcomes']) == 0: return 
        results = []
        for outcome in data['outcomes']:
            if str(outcome) != '':
                results.append(str(outcome))
            else:
                logging.error("E")
                return False
        return results

    def fetch_feed_info(self, feed):
        # sanity check
        if feed['info_status'] != 'needfetch': return False
        if 'info_url' not in feed: return False
        if not util.is_valid_url(feed['info_url'], suffix='.json'): return False

        logging.info('Fetching feed informations: '+feed['info_url'])

        data = util.fetch_json(feed['info_url'])
        if not data: 
            logging.info('Fetching error: '+feed['info_url'])
            self.inc_fetch_retry(feed, errors=['Fetch json failed'])
            return False

        logging.info('Fetched data:')
        logging.info(data)

        deadlines = self.parse_deadlines(data)
        outcomes = self.parse_outcomes(data)
        date = util.date_param(data['date'])

        # required fields
        errors = []
        if 'owner' not in data or data['owner'] == '': errors.append("Invalid owner")
        if 'event' not in data or data['event'] == '': errors.append("Invalid event")
        if 'address' not in data or data['address'] != feed['source']: errors.append("Invalid address")
        if 'category' not in data or data['category'] not in config.FEED_CATEGORIES : errors.append("Invalid category")
        if 'type' not in data or data['type'] not in config.FEED_TYPES: errors.append("Invalid type")
        if not deadlines: errors.append("Invalid deadlines")
        if not outcomes: errors.append("Invalid outcomes")
        if not date: errors.append("Invalid date")
        
        if len(errors)>0:
            logging.info('Invalid json: '+feed['info_url'])
            self.inc_fetch_retry(feed, new_status = 'invalid', errors=errors) 
            return False        

        feed['info_status'] = 'valid'
        feed['owner'] = util.sanitize_eliteness(data['owner'])[0:60]
        feed['event'] = util.sanitize_eliteness(data['event'])[0:255]
        feed['category'] = data['category']
        feed['type'] = data['type']
        feed['deadlines'] = deadlines
        feed['outcomes'] = outcomes
        feed['date'] = date

        # optional fields
        feed['website'] = ''
        feed['with_image'] = False
        feed['errors'] = []
        if 'website' in data and util.is_valid_url(data['website']):
            feed['website'] = data['website']
        if 'image' in data and util.is_valid_url(data['image'], suffix='.png'): 
            if util.fetch_image(data['image'], config.SUBDIR_FEED_IMAGES, feed['source']):
                feed['with_image'] = True
            else:
                feed['errors'] = ['Fetch image failed']

        logging.info('Save feed:')
        logging.info(feed)

        self.db.feeds.save(feed)

    def fetch_all_feed_info(self):
        feeds = self.db.feeds.find({'info_status': 'needfetch'})
        for feed in feeds:
            self.fetch_feed_info(feed)

    def find_feeds(self, bet_type='simple', category='', owner='', source='', sort_order=-1):
        conditions = { 
            'type': bet_type, 
            'info_status': 'valid'
        }    
        if source != '': 
            conditions['source'] = source
        else:
            if category != '' and category in config.FEED_CATEGORIES: conditions['category'] = category
            if owner != '': conditions['owner'] = owner

        if sort_order not in [1, -1]: sort_order = -1

        results = []
        try:  
            feeds = self.db.feeds.find(spec=conditions, fields={'_id': False}, sort=[('deadline', sort_order)], limit=50)
            for feed in feeds:
                feed['equal_odd'] = self.get_odd(2, feed['source'])[0]
                feed['not_equal_odd'] = self.get_odd(3, feed['source'])[0]
                results.append(feed)          
        except Exception, e:
            logging.error(e)

        return results

    def get_odd(self, bet_type, feed_address, target_value=1, leverage=5040):
        sql  = "SELECT SUM(wager_quantity) AS wager, SUM(counterwager_quantity) AS counterwager "
        sql += "FROM bets WHERE status='open' "
        sql += "AND bet_type=? AND feed_address=? AND target_value=? AND leverage=?"
        bindings = (bet_type, feed_address, target_value, leverage)
        return util.counterpartyd_query(sql, bindings);

    def find_bets(self, bet_type, feed_address, target_value=1, leverage=5040):      
        sql  = "SELECT * FROM bets WHERE status='open' AND "
        sql += "bet_type=? AND feed_address=? AND target_value=? AND leverage=? "
        sql += "ORDER BY deadline LIMIT 50"
        bindings = (bet_type, feed_address, target_value, leverage)        
        return util.counterpartyd_query(sql, bindings);


        
        

