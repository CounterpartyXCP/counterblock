from lib import config, util
from datetime import datetime
import sqlite3
import logging
import decimal

D = decimal.Decimal

class Betting:

    def __init__(self, mongo_db):
        self.db = mongo_db

    def parse_broadcast(self, message):
        logging.info('Parsing broadcast message..')
        logging.info(message)

        save = False
        feed = self.db.feeds.find_one({"source": message['source']})
        
        if util.is_valid_url(message['text'], suffix='.json') and message['value'] == -1.0:
            if feed is None: feed = {}
            feed['source'] = message['source']
            feed['info_url'] = message['text']
            feed['info_status'] = 'needfetch' #needfetch, valid (included in CW feed directory), invalid, error 
            feed['fetch_info_retry'] = 0 # retry 3 times to fetch info from info_url
            feed['info_data'] = {}
            feed['fee_fraction_int'] = message['fee_fraction_int']
            feed['locked'] = False
            feed['last_broadcast'] = {}
            save = True
        elif feed is not None:
            if message['locked']:
                feed['locked'] = True
            else:
                feed['last_broadcast'] = {
                    "text": message['text'],
                    "value": message['value']
                }
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

    def sanitize_json_data(self, data):
        # TODO: make this in more elegant way
        data['owner']['name'] = util.sanitize_eliteness(data['owner']['name'])
        if 'description' in data['owner']: data['owner']['description'] = util.sanitize_eliteness(data['owner']['description'])
        data['topic']['name'] = util.sanitize_eliteness(data['topic']['name'])
        if 'description' in data['topic']: data['topic']['description'] = util.sanitize_eliteness(data['topic']['description'])
        for i in range(len(data["targets"])):
            data["targets"][i]['text'] = util.sanitize_eliteness(data["targets"][i]['text'])
            if 'description' in data["targets"][i]: data["targets"][i]['description'] = util.sanitize_eliteness(data["targets"][i]['description'])
            if 'labels' in data["targets"][i]:
                data["targets"][i]['labels']['equal'] = util.sanitize_eliteness(data["targets"][i]['labels']['equal'])
                data["targets"][i]['labels']['not_equal'] = util.sanitize_eliteness(data["targets"][i]['labels']['not_equal'])
        if 'customs' in data:
            for key in data['customs']:
                if isinstance(data['customs'][key], str): data['customs'][key] = util.sanitize_eliteness(data['customs'][key])
        return data

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

        errors = util.is_valid_json(data, config.FEED_SCHEMA)

        if feed['source'] != data['address']:
            errors.append("Invalid address")
       
        if len(errors)>0:
            logging.info('Invalid json: '+feed['info_url'])
            self.inc_fetch_retry(feed, new_status = 'invalid', errors=errors) 
            return False        

        feed['info_status'] = 'valid'

        if 'image' in data['owner']:
            data['owner']['valid_image'] = util.fetch_image(data['owner']['image'], config.SUBDIR_FEED_IMAGES, feed['source']+'_owner')
        
        if 'image' in data['topic']:
            data['topic']['valid_image'] = util.fetch_image(data['topic']['image'], config.SUBDIR_FEED_IMAGES, feed['source']+'_topic')

        for i in range(len(data["targets"])):
            if 'image' in data["targets"][i]:
                image_name = feed['source']+'_tv_'+str(data["targets"][i]['value'])
                data["targets"][i]['valid_image'] = util.fetch_image(data["targets"][i]['image'], config.SUBDIR_FEED_IMAGES, image_name)

        if 'deadline' in data:
            if 'deadlines' not in data: data["deadlines"] = []
            data["deadlines"].insert(0, data["deadline"])

        
        
        feed['info_data'] = self.sanitize_json_data(data)

        logging.info('Save feed:')
        logging.info(feed)

        self.db.feeds.save(feed)

    def fetch_all_feed_info(self):
        feeds = self.db.feeds.find({'info_status': 'needfetch'})
        for feed in feeds:
            self.fetch_feed_info(feed)

    # TODO: counter cache
    def get_feed_counters(self, feed_address):
        counters = {}        
        sql  = "SELECT COUNT(*) AS bet_count, SUM(wager_quantity) AS wager_quantity, SUM(wager_remaining) AS wager_remaining, status FROM bets "
        sql += "WHERE feed_address=? GROUP BY status ORDER BY status DESC"
        bindings = (feed_address,)        
        counters['bets'] = util.counterpartyd_query(sql, bindings)
        return counters;

    def find_feed(self, url_or_address):
        conditions = {
            '$or': [{'source': url_or_address}, {'info_url': url_or_address}],
            'info_status': 'valid'
        }
        result = {}
        feeds = self.db.feeds.find(spec=conditions, fields={'_id': False}, imit=1)
        for feed in feeds:
            result = feed;
            result['counters'] = self.get_feed_counters(feed['source'])

        return result

    # not used
    def find_feeds(self, bet_type='simple', category='', owner='', source='', sort_order=-1, url=''):
        if url != '':
            conditions = { 
                'info_url': url, 
                'info_status': 'valid'
            }
            limit = 1
        else:
            conditions = { 
                'type': bet_type, 
                'info_status': 'valid'
            }    
            if source != '': 
                conditions['source'] = source
            else:
                if category != '' and category in config.FEED_CATEGORIES: conditions['category'] = category
                if owner != '': conditions['owner'] = owner
            limit = 50

        if sort_order not in [1, -1]: sort_order = -1

        results = []
        try:  
            feeds = self.db.feeds.find(spec=conditions, fields={'_id': False}, sort=[('date', sort_order)], limit=50)
            for feed in feeds:
                feed['odds'] = []
                target = 0
                for t in feed['targets']:
                    target += 1
                    equal_bet = self.find_bets(3, feed['source'], str(feed['deadlines'][0]), target, limit=1)
                    not_equal_bet = self.find_bets(2, feed['source'], str(feed['deadlines'][0]), target, limit=1)
                    odd = {'equal': 'NA', 'not_equal': 'NA'}
                    if len(equal_bet) > 0:
                        odd['equal'] = float(equal_bet[0]['wager_quantity']) / float(equal_bet[0]['counterwager_quantity'])
                    if len(not_equal_bet) > 0:
                        odd['not_equal'] = float(not_equal_bet[0]['wager_quantity']) / float(not_equal_bet[0]['counterwager_quantity'])
                    feed['odds'].append(odd)                   
                results.append(feed)          
        except Exception, e:
            logging.error(e)

        return results

    def find_bets(self, bet_type, feed_address, deadline, target_value=1, leverage=5040, limit=50):         
        sql  = "SELECT * FROM bets WHERE counterwager_remaining>0 AND "
        sql += "bet_type=? AND feed_address=? AND target_value=? AND leverage=? AND deadline=? "
        sql += "ORDER BY ((counterwager_quantity+0.0)/(wager_quantity+0.0)) ASC LIMIT ?";
        bindings = (bet_type, feed_address, target_value, leverage, deadline, limit)        
        return util.counterpartyd_query(sql, bindings);

    def find_user_bets(self, addresses, status='open'):
        logging.error(addresses)
        ins = []

        sql  = "SELECT * FROM bets WHERE status = ? AND source IN ("+",".join(['?' for e in range(0,len(addresses))])+") ";
        sql += "ORDER BY tx_index DESC LIMIT 100"
        bindings = (status,) + tuple(addresses)
        bets = util.counterpartyd_query(sql, bindings);

        sources = {}
        for bet in bets: sources[bet['feed_address']] = True;
        conditions = { 'source': { '$in': sources.keys() }}
        feeds = self.db.feeds.find(spec=conditions, fields={'_id': False})
        feedsBySource = {}
        for feed in feeds: feedsBySource[feed['source']] = feed

        return {
            'bets': bets,
            'feeds': feedsBySource
        }
        
        

