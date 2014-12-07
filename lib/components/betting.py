from datetime import datetime
import logging
import decimal
import base64
import json

from lib import config, util

FEED_MAX_RETRY = 3
D = decimal.Decimal

def parse_broadcast(db, message):
    save = False
    feed = db.feeds.find_one({'source': message['source']})
    
    if util.is_valid_url(message['text'], allow_no_protocol=True) and message['value'] == -1.0:
        if feed is None: 
            feed = {}
        feed['source'] = message['source']
        feed['info_url'] = message['text']
        feed['info_status'] = 'needfetch' #needfetch, valid (included in CW feed directory), invalid, error 
        feed['fetch_info_retry'] = 0 # retry FEED_MAX_RETRY times to fetch info from info_url
        feed['info_data'] = {}
        feed['fee_fraction_int'] = message['fee_fraction_int']
        feed['locked'] = False
        feed['last_broadcast'] = {}
        feed['errors'] = []
        save = True
    elif feed is not None:
        if message['locked']:
            feed['locked'] = True
        else:
            feed['last_broadcast'] = {
                'text': message['text'],
                'value': message['value']
            }
            feed['fee_fraction_int'] = message['fee_fraction_int']
        save = True

    if save:  
        db.feeds.save(feed)
        return True
    return False

def inc_fetch_retry(db, feed, max_retry=FEED_MAX_RETRY, new_status='error', errors=[]):
    feed['fetch_info_retry'] += 1
    feed['errors'] = errors
    if feed['fetch_info_retry'] == max_retry:
        feed['info_status'] = new_status
    db.feeds.save(feed)

def sanitize_json_data(data):
    if 'operator' in data:
        data['operator']['name'] = util.sanitize_eliteness(data['operator']['name'])
        if 'description' in data['operator']: data['operator']['description'] = util.sanitize_eliteness(data['operator']['description'])
    data['title'] = util.sanitize_eliteness(data['title'])
    if 'description' in data: data['description'] = util.sanitize_eliteness(data['description'])
    if 'targets' in data:
        for i in range(len(data['targets'])):
            data['targets'][i]['text'] = util.sanitize_eliteness(data['targets'][i]['text'])
            if 'description' in data['targets'][i]: data['targets'][i]['description'] = util.sanitize_eliteness(data['targets'][i]['description'])
            if 'labels' in data['targets'][i]:
                data['targets'][i]['labels']['equal'] = util.sanitize_eliteness(data['targets'][i]['labels']['equal'])
                data['targets'][i]['labels']['not_equal'] = util.sanitize_eliteness(data['targets'][i]['labels']['not_equal'])
    if 'customs' in data:
        for key in data['customs']:
            if isinstance(data['customs'][key], str): data['customs'][key] = util.sanitize_eliteness(data['customs'][key])
    return data

def process_feed_info(db, feed, info_data):
    # sanity check
    assert feed['info_status'] == 'needfetch'
    assert 'info_url' in feed
    assert util.is_valid_url(feed['info_url'], allow_no_protocol=True) #already validated in the fetch

    errors = util.is_valid_json(info_data, config.FEED_SCHEMA)
    
    if not isinstance(info_data, dict) or 'address' not in info_data:
        errors.append('Invalid data format')
    elif feed['source'] != info_data['address']:
        errors.append('Invalid address')
   
    if len(errors) > 0:
        inc_fetch_retry(db, feed, new_status='invalid', errors=errors)
        return (False, errors) 

    feed['info_status'] = 'valid'

    #fetch any associated images...
    #TODO: parallelize this 2nd level feed image fetching ... (e.g. just compose a list here, and process it in later on)
    if 'image' in info_data:
        info_data['valid_image'] = util.fetch_image(info_data['image'],
            config.SUBDIR_FEED_IMAGES, feed['source'] + '_topic', fetch_timeout=5)
    if 'operator' in info_data and 'image' in info_data['operator']:
        info_data['operator']['valid_image'] = util.fetch_image(info_data['operator']['image'],
            config.SUBDIR_FEED_IMAGES, feed['source'] + '_owner', fetch_timeout=5)
    if 'targets' in info_data:
        for i in range(len(info_data['targets'])):
            if 'image' in info_data['targets'][i]:
                image_name = feed['source'] + '_tv_' + str(info_data['targets'][i]['value'])
                info_data['targets'][i]['valid_image'] = util.fetch_image(
                    info_data['targets'][i]['image'], config.SUBDIR_FEED_IMAGES, image_name, fetch_timeout=5)

    feed['info_data'] = sanitize_json_data(info_data)
    db.feeds.save(feed)
    return (True, None)

def fetch_all_feed_info(db):
    feeds = list(db.feeds.find({'info_status': 'needfetch'}))
    feed_info_urls = []

    def feed_fetch_complete_hook(urls_data):
        logging.info("Enhanced feed info fetching complete. %s unique URLs fetched. Processing..." % len(urls_data))
        feeds = db.feeds.find({'info_status': 'needfetch'})
        for feed in feeds:
            #logging.debug("Looking at feed %s: %s" % (feed, feed['info_url']))
            if feed['info_url']:
                info_url = ('http://' + feed['info_url']) \
                    if not feed['info_url'].startswith('http://') and not feed['info_url'].startswith('https://') else feed['info_url']
                if info_url not in urls_data:
                    logging.warn("URL %s not properly fetched (not one of %i entries in urls_data), skipping..." % (info_url, len(urls_data)))
                    continue
                assert info_url in urls_data
                if not urls_data[info_url][0]: #request was not successful
                    inc_fetch_retry(db, feed, max_retry=FEED_MAX_RETRY, errors=[urls_data[info_url][1]])
                    logging.warn("Fetch for feed at %s not successful: %s (try %i of %i)" % (
                        info_url, urls_data[info_url][1], feed['fetch_info_retry'], FEED_MAX_RETRY))
                else:
                    result = process_feed_info(db, feed, urls_data[info_url][1])
                    if not result[0]:
                        logging.info("Processing for feed at %s not successful: %s" % (info_url, result[1]))
                    else:
                        logging.info("Processing for feed at %s successful" % info_url)
        
    #compose and fetch all info URLs in all feeds with them
    for feed in feeds:
        assert feed['info_url']
        feed_info_urls.append(('http://' + feed['info_url']) \
            if not feed['info_url'].startswith('http://') and not feed['info_url'].startswith('https://') else feed['info_url'])
    feed_info_urls_str = ', '.join(feed_info_urls)
    feed_info_urls_str = (feed_info_urls_str[:2000] + ' ...') if len(feed_info_urls_str) > 2000 else feed_info_urls_str #truncate if necessary
    if len(feed_info_urls):
        logging.info('Fetching enhanced feed info for %i feeds: %s' % (len(feed_info_urls), feed_info_urls_str))
        util.stream_fetch(feed_info_urls, feed_fetch_complete_hook,
            fetch_timeout=10, max_fetch_size=4*1024, urls_group_size=20, urls_group_time_spacing=20,
            per_request_complete_callback=lambda url, data: logging.debug("Feed at %s retrieved, result: %s" % (url, data)))

def get_feed_counters(feed_address):
    counters = {}        
    sql  = 'SELECT COUNT(*) AS bet_count, SUM(wager_quantity) AS wager_quantity, SUM(wager_remaining) AS wager_remaining, status FROM bets '
    sql += 'WHERE feed_address=? GROUP BY status ORDER BY status DESC'
    bindings = [feed_address] 
    params = {
        'query': sql,
        'bindings': bindings
    }       
    counters['bets'] = util.call_jsonrpc_api('sql', params)['result']
    return counters;

def find_feed(db, url_or_address):
    conditions = {
        '$or': [{'source': url_or_address}, {'info_url': url_or_address}],
        'info_status': 'valid'
    }
    result = {}
    feeds = db.feeds.find(spec=conditions, fields={'_id': False}, limit=1)
    for feed in feeds:
        if 'targets' not in feed['info_data'] or ('type' in feed['info_data'] and feed['info_data']['type'] in ['all', 'cfd']):
            feed['info_data']['next_broadcast'] = util.next_interval_date(feed['info_data']['broadcast_date'])
            feed['info_data']['next_deadline'] = util.next_interval_date(feed['info_data']['deadline'])
        result = feed
        result['counters'] = get_feed_counters(feed['source'])
    
    if 'counters' not in result:
        params = {
            'filters': {
                'field': 'source',
                'op': '=',
                'value': url_or_address
            },
            'order_by': 'tx_index',
            'order_dir': 'DESC',
            'limit': 10
        }
        broadcasts = util.call_jsonrpc_api('get_broadcasts', params)['result']
        if broadcasts:
            return {
                'broadcasts': broadcasts,
                'counters': get_feed_counters(url_or_address)
            }
      
    return result

def parse_base64_feed(base64_feed):
    decoded_feed = base64.b64decode(base64_feed)
    feed = json.loads(decoded_feed)
    if isinstance(feed, dict) and 'feed' in feed:
        errors = util.is_valid_json(feed['feed'], config.FEED_SCHEMA)
        if len(errors) > 0:
            raise Exception("Invalid json: {}".format(", ".join(errors)))
        # get broadcast infos
        params = {
            'filters': {
                'field': 'source',
                'op': '=',
                'value': feed['feed']['address']
            },
            'order_by': 'tx_index',
            'order_dir': 'DESC',
            'limit': 1
        }
        broadcasts = util.call_jsonrpc_api('get_broadcasts', params)['result']
        if len(broadcasts) == 0:
            raise Exception("invalid feed address")

        complete_feed = {}
        complete_feed['fee_fraction_int'] = broadcasts[0]['fee_fraction_int']
        complete_feed['source'] = broadcasts[0]['source']
        complete_feed['locked'] = broadcasts[0]['locked']
        complete_feed['counters'] = get_feed_counters(broadcasts[0]['source'])
        complete_feed['info_data'] = sanitize_json_data(feed['feed'])
        
        feed['feed'] = complete_feed
        return feed

def find_bets(bet_type, feed_address, deadline, target_value=None, leverage=5040, limit=50):  
    bindings = []       
    sql  = 'SELECT * FROM bets WHERE counterwager_remaining>0 AND '
    sql += 'bet_type=? AND feed_address=? AND leverage=? AND deadline=? '
    bindings += [bet_type, feed_address, leverage, deadline]
    if target_value != None:
        sql += 'AND target_value=? '
        bindings.append(target_value)
    sql += 'ORDER BY ((counterwager_quantity+0.0)/(wager_quantity+0.0)) ASC LIMIT ?';
    bindings.append(limit)
    params = {
        'query': sql,
        'bindings': bindings
    }   
    return util.call_jsonrpc_api('sql', params)['result']

def get_feeds_by_source(db, addresses):
    conditions = { 'source': { '$in': addresses }}
    feeds = db.feeds.find(spec=conditions, fields={'_id': False})
    feeds_by_source = {}
    for feed in feeds: feeds_by_source[feed['source']] = feed
    return feeds_by_source

# TODO: move this in Counterwallet
def find_user_bets(db, addresses, status='open'):
    params = {
        'filters': {
            'field': 'source',
            'op': 'IN',
            'value': addresses
        },
        'status': status,
        'order_by': 'tx_index',
        'order_dir': 'DESC',
        'limit': 100
    }
    bets = util.call_jsonrpc_api('get_bets', params)['result']

    sources = {}
    for bet in bets:
        sources[bet['feed_address']] = True
    
    return {
        'bets': bets,
        'feeds': get_feeds_by_source(db, sources.keys())
    }
