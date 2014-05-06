from lib import config, util
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

    def parse_options(self, data):
        if 'options' not in data: return False
        if not isinstance(data['options'], list): return False
        if len(data['options']) < 1: return False

        valid_options = []
        for option in data['options']:
            if 'name' not in option or option['name'] == '': continue
            if 'value' not in option or not isinstance(option['value'], (int, float)): continue
            valid_option = {
                'name': util.sanitize_eliteness(option['name'])[0:60],
                'value': option['value'],
                'description': ''
            }
            if 'description' in option:
                valid_option['description'] = util.sanitize_eliteness(option['name'])[0:255]
            valid_options.append(valid_option)

        if len(valid_options) == 0: return False
        return valid_options

    def inc_fetch_retry(self, feed, max_retry = 3, new_status = 'error', errors=[]):
        feed['fetch_info_retry'] += 1
        feed['errors'] = errors
        if feed['fetch_info_retry'] == max_retry:
            feed['info_status'] = new_status
        self.db.feeds.save(feed)

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

        # required fields
        errors = []
        options = self.parse_options(data)
        deadline = util.date_param(data['deadline'])
        if 'owner' not in data or data['owner'] == '': errors.append("Invalid owner")
        if 'name' not in data or data['name'] == '': errors.append("Invalid name")
        if 'address' not in data or data['address'] != feed['source']: errors.append("Invalid address")
        if 'category' not in data or data['category'] not in config.FEED_CATEGORIES : errors.append("Invalid category")
        if 'type' not in data or data['type'] not in config.FEED_TYPES: errors.append("Invalid type")
        if 'deadline' not in data or  deadline == False: errors.append("Invalid deadline")
        if options == False: errors.append("Invalid options")
        
        if len(errors)>0:
            logging.info('Invalid json: '+feed['info_url'])
            self.inc_fetch_retry(feed, new_status = 'invalid', errors=errors) 
            return False        

        feed['info_status'] = 'valid'
        feed['owner'] = util.sanitize_eliteness(data['owner'])[0:60]
        feed['name'] = util.sanitize_eliteness(data['name'])[0:60]
        feed['category'] = data['category']
        feed['type'] = data['type']
        feed['deadline'] = util.date_param(data['deadline'])
        feed['options'] = options

        # optional fields
        feed['description'] = feed['website'] = ''
        feed['with_image'] = False
        feed['errors'] = []
        if 'description' in data:
            feed['description'] = util.sanitize_eliteness(data['description'])[0:2048]
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
        conditions = { 'type':bet_type, 'info_status': 'valid' }    
        if source != '': 
            conditions['source'] = source
        else:
            if category != '' and category in config.FEED_CATEGORIES: conditions['category'] = category
            if owner != '': conditions['owner'] = owner

        if sort_order not in [1, -1]: sort_order = -1

        results = []
        try:  
            feeds = self.db.feeds.find(spec=conditions, fields={'_id': False}, sort=[('deadline', sort_order)], limit=50)
            for feed in feeds: results.append(feed)          
        except Exception, e:
            logging.error(e)

        return results
        

