import os
import re
import json
import base64
import logging
import datetime
import time
import copy
import decimal
import cgi
import itertools
import StringIO
import subprocess

import gevent
import gevent.pool
import gevent.ssl
import numpy
import pymongo
from geventhttpclient import HTTPClient
from geventhttpclient.url import URL
import lxml.html
from PIL import Image

import dateutil.parser
import calendar
import pygeoip
import hashlib

from jsonschema import FormatChecker, Draft4Validator, FormatError
# not needed here but to ensure that installed
import strict_rfc3339, rfc3987, aniso8601

from lib import config, util_bitcoin

JSONRPC_API_REQUEST_TIMEOUT = 10 #in seconds 
D = decimal.Decimal


def sanitize_eliteness(text):
    #strip out html data to avoid XSS-vectors
    return cgi.escape(lxml.html.document_fromstring(text).text_content())
    #^ wrap in cgi.escape - see https://github.com/mitotic/graphterm/issues/5

def http_basic_auth_str(username, password):
    """Returns a Basic Auth string."""
    authstr = 'Basic ' + str(base64.b64encode(('%s:%s' % (username, password)).encode('latin1')).strip())
    return authstr

def is_valid_url(url, suffix='', allow_localhost=False, allow_no_protocol=False):
    if url is None:
        return False

    regex = re.compile(
        r'^https?://' if not allow_no_protocol else r'^(https?://)?' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)%s$' % (re.escape('%s') % suffix if suffix else ''), re.IGNORECASE)
    
    if not allow_localhost:
        if re.search(r'^https?://localhost', url, re.IGNORECASE) or re.search(r'^https?://127', url, re.IGNORECASE):
            return None
    
    return regex.search(url)

def assets_to_asset_pair(asset1, asset2):
    base, quote = None, None
    
    for quote_asset in config.QUOTE_ASSETS:
        if asset1 == quote_asset or asset2 == quote_asset:
            base, quote = (asset2, asset1) if asset1 == quote_asset else (asset1, asset2) 
            break
    else:
        base, quote = (asset1, asset2) if asset1 < asset2 else (asset2, asset1)
        
    return (base, quote)

def call_jsonrpc_api(method, params=None, endpoint=None, auth=None, abort_on_error=False):
    if not endpoint: endpoint = config.COUNTERPARTYD_RPC
    if not auth: auth = config.COUNTERPARTYD_AUTH
    if not params: params = {}
    
    payload = {
      "id": 0,
      "jsonrpc": "2.0",
      "method": method,
      "params": params,
    }
    headers = {
        'Content-Type': 'application/json',
        'Connection':'close', #no keepalive
    }
    if auth:
        #auth should be a (username, password) tuple, if specified
        headers['Authorization'] = http_basic_auth_str(auth[0], auth[1])
    
    try:
        u = URL(endpoint)
        client = HTTPClient.from_url(u, connection_timeout=JSONRPC_API_REQUEST_TIMEOUT,
            network_timeout=JSONRPC_API_REQUEST_TIMEOUT)
        r = client.post(u.request_uri, body=json.dumps(payload), headers=headers)
    except Exception, e:
        raise Exception("Got call_jsonrpc_api request error: %s" % e)
    else:
        if r.status_code != 200 and abort_on_error:
            raise Exception("Bad status code returned from counterpartyd: '%s'. result body: '%s'." % (r.status_code, r.read()))
        result = json.loads(r.read())
    finally:
        client.close()
    
    if abort_on_error and 'error' in result:
        raise Exception("Got back error from server: %s" % result['error'])
    return result

def get_url(url, abort_on_error=False, is_json=True, fetch_timeout=5):
    headers = { 'Connection':'close', } #no keepalive

    try:
        u = URL(url)
        client_kwargs = {'connection_timeout': fetch_timeout, 'network_timeout': fetch_timeout, 'insecure': True}
        if u.scheme == "https": client_kwargs['ssl_options'] = {'cert_reqs': gevent.ssl.CERT_NONE}
        client = HTTPClient.from_url(u, **client_kwargs)
        r = client.get(u.request_uri, headers=headers)
    except Exception, e:
        raise Exception("Got get_url request error: %s" % e)
    else:
        if r.status_code != 200 and abort_on_error:
            raise Exception("Bad status code returned: '%s'. result body: '%s'." % (r.status_code, r.read()))
        result = json.loads(r.read()) if is_json else r.read()
    finally:
        client.close()
    return result

def get_address_cols_for_entity(entity):
    if entity in ['debits', 'credits']:
        return ['address',]
    elif entity in ['issuances',]:
        return ['issuer',]
    elif entity in ['sends', 'dividends', 'bets', 'cancels', 'callbacks', 'orders', 'burns', 'broadcasts', 'btcpays']:
        return ['source',]
    #elif entity in ['order_matches', 'bet_matches']:
    elif entity in ['order_matches', 'order_expirations', 'order_match_expirations',
                    'bet_matches', 'bet_expirations', 'bet_match_expirations']:
        return ['tx0_address', 'tx1_address']
    else:
        raise Exception("Unknown entity type: %s" % entity)

def grouper(n, iterable, fillmissing=False, fillvalue=None):
    #Modified from http://stackoverflow.com/a/1625013
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    data = itertools.izip_longest(*args, fillvalue=fillvalue)
    if not fillmissing:
        data = [[e for e in g if e != fillvalue] for g in data]
    return data

def multikeysort(items, columns):
    """http://stackoverflow.com/a/1144405"""
    from operator import itemgetter
    comparers = [ ((itemgetter(col[1:].strip()), -1) if col.startswith('-') else (itemgetter(col.strip()), 1)) for col in columns]  
    def comparer(left, right):
        for fn, mult in comparers:
            result = cmp(fn(left), fn(right))
            if result:
                return mult * result
        else:
            return 0
    return sorted(items, cmp=comparer)

def moving_average(samples, n=3) :
    ret = numpy.cumsum(samples, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n - 1:] / n

def weighted_average(value_weight_list):
    """Takes a list of tuples (value, weight) and returns weighted average as
    calculated by Sum of all values * weights / Sum of all weights
    http://bcdcspatial.blogspot.com/2010/08/simple-weighted-average-with-python.html
    """    
    numerator = sum([v * w for v,w in value_weight_list])
    denominator = sum([w for v,w in value_weight_list])
    if(denominator != 0):
        return(float(numerator) / float(denominator))
    else:
        return None

def json_dthandler(obj):
    if hasattr(obj, 'timetuple'): #datetime object
        #give datetime objects to javascript as epoch ts in ms (i.e. * 1000)
        return int(time.mktime(obj.timetuple())) * 1000
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))

def get_block_indexes_for_dates(start_dt=None, end_dt=None):
    """Returns a 2 tuple (start_block, end_block) result for the block range that encompasses the given start_date
    and end_date unix timestamps"""
    mongo_db = config.mongo_db
    if start_dt is None:
        start_block_index = config.BLOCK_FIRST
    else:
        start_block = mongo_db.processed_blocks.find_one({"block_time": {"$lte": start_dt} }, sort=[("block_time", pymongo.DESCENDING)])
        start_block_index = config.BLOCK_FIRST if not start_block else start_block['block_index']
    
    if end_dt is None:
        end_block_index = config.CURRENT_BLOCK_INDEX
    else:
        end_block = mongo_db.processed_blocks.find_one({"block_time": {"$gte": end_dt} }, sort=[("block_time", pymongo.ASCENDING)])
        if not end_block:
            end_block_index = mongo_db.processed_blocks.find_one(sort=[("block_index", pymongo.DESCENDING)])['block_index']
        else:
            end_block_index = end_block['block_index']
    return (start_block_index, end_block_index)

def get_block_time(block_index):
    """TODO: implement result caching to avoid having to go out to the database"""
    block = config.mongo_db.processed_blocks.find_one({"block_index": block_index })
    if not block: return None
    return block['block_time']

def decorate_message(message, for_txn_history=False):
    #insert custom fields in certain events...
    #even invalid actions need these extra fields for proper reporting to the client (as the reporting message
    # is produced via PendingActionViewModel.calcText) -- however make it able to deal with the queried data not existing in this case
    assert '_category' in message
    mongo_db = config.mongo_db
    if for_txn_history:
        message['_command'] = 'insert' #history data doesn't include this
        block_index = message['block_index'] if 'block_index' in message else message['tx1_block_index']
        message['_block_time'] = get_block_time(block_index)
        message['_tx_index'] = message['tx_index'] if 'tx_index' in message else message.get('tx1_index', None)  
        if message['_category'] in ['bet_expirations', 'order_expirations', 'bet_match_expirations', 'order_match_expirations']:
            message['_tx_index'] = 0 #add tx_index to all entries (so we can sort on it secondarily in history view), since these lack it
    
    if message['_category'] in ['credits', 'debits']:
        #find the last balance change on record
        bal_change = mongo_db.balance_changes.find_one({ 'address': message['address'], 'asset': message['asset'] },
            sort=[("block_time", pymongo.DESCENDING)])
        message['_quantity_normalized'] = abs(bal_change['quantity_normalized']) if bal_change else None
        message['_balance'] = bal_change['new_balance'] if bal_change else None
        message['_balance_normalized'] = bal_change['new_balance_normalized'] if bal_change else None

    if message['_category'] in ['orders',] and message['_command'] == 'insert':
        get_asset_info = mongo_db.tracked_assets.find_one({'asset': message['get_asset']})
        give_asset_info = mongo_db.tracked_assets.find_one({'asset': message['give_asset']})
        message['_get_asset_divisible'] = get_asset_info['divisible'] if get_asset_info else None
        message['_give_asset_divisible'] = give_asset_info['divisible'] if give_asset_info else None
    
    if message['_category'] in ['order_matches',] and message['_command'] == 'insert':
        forward_asset_info = mongo_db.tracked_assets.find_one({'asset': message['forward_asset']})
        backward_asset_info = mongo_db.tracked_assets.find_one({'asset': message['backward_asset']})
        message['_forward_asset_divisible'] = forward_asset_info['divisible'] if forward_asset_info else None
        message['_backward_asset_divisible'] = backward_asset_info['divisible'] if backward_asset_info else None
    
    if message['_category'] in ['orders', 'order_matches',]:
        message['_btc_below_dust_limit'] = (
                ('forward_asset' in message and message['forward_asset'] == config.BTC and message['forward_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF)
             or ('backward_asset' in message and message['backward_asset'] == config.BTC and message['backward_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF)
        )

    if message['_category'] in ['dividends', 'sends', 'callbacks']:
        asset_info = mongo_db.tracked_assets.find_one({'asset': message['asset']})
        message['_divisible'] = asset_info['divisible'] if asset_info else None
    
    if message['_category'] in ['issuances',]:
        message['_quantity_normalized'] = util_bitcoin.normalize_quantity(message['quantity'], message['divisible'])
    return message

def decorate_message_for_feed(msg, msg_data=None):
    """This function takes a message from counterpartyd's message feed and mutates it a bit to be suitable to be
    sent through the counterblockd message feed to an end-client"""
    if not msg_data:
        msg_data = json.loads(msg['bindings'])
    
    message = copy.deepcopy(msg_data)
    message['_message_index'] = msg['message_index']
    message['_command'] = msg['command']
    message['_block_index'] = msg['block_index']
    message['_block_time'] = get_block_time(msg['block_index'])
    message['_category'] = msg['category']
    message['_status'] = msg_data.get('status', 'valid')
    message = decorate_message(message)
    return message

def is_caught_up_well_enough_for_government_work():
    """We don't want to give users 525 errors or login errors if counterblockd/counterpartyd is in the process of
    getting caught up, but we DO if counterblockd is either clearly out of date with the blockchain, or reinitializing its database"""
    return config.CAUGHT_UP or (config.BLOCKCHAIN_SERVICE_LAST_BLOCK and config.CURRENT_BLOCK_INDEX >= config.BLOCKCHAIN_SERVICE_LAST_BLOCK - 1)

def stream_fetch(urls, completed_callback, urls_group_size=50, urls_group_time_spacing=0, max_fetch_size=4*1024,
fetch_timeout=1, is_json=True, per_request_complete_callback=None):
    completed_urls = {}
    
    def make_stream_request(url):
        try:
            u = URL(url)
            client_kwargs = {'connection_timeout': fetch_timeout, 'network_timeout': fetch_timeout, 'insecure': True}
            if u.scheme == "https": client_kwargs['ssl_options'] = {'cert_reqs': gevent.ssl.CERT_NONE}
            client = HTTPClient.from_url(u, **client_kwargs)
            r = client.get(u.request_uri, headers={'Connection':'close'})
        except Exception, e:
            data = (False, "Got exception: %s" % e)
        else:
            if r.status_code != 200:
                data = (False, "Got non-successful response code of: %s" % r.status_code)
            else:
                try:
                    #read up to max_fetch_size
                    raw_data = r.read(max_fetch_size)
                    if is_json: #try to convert to JSON
                        try:
                            data = json.loads(raw_data)
                        except Exception, e:
                            data = (False, "Invalid JSON data: %s" % e)
                        else:
                            data = (True, data)
                    else: #keep raw
                        data = (True, raw_data)
                except Exception, e:
                    data = (False, "Request error: %s" % e)
        finally:
            client.close()
            
        if per_request_complete_callback:
            per_request_complete_callback(url, data)
            
        completed_urls[url] = data
        if len(completed_urls) == len(urls): #all done, trigger callback
            return completed_callback(completed_urls)
        
    def process_group(group):
        group_results = []
        pool = gevent.pool.Pool(urls_group_size)
        for url in group:
            if not is_valid_url(url, allow_no_protocol=True):
                completed_urls[url] = (False, "Invalid URL")
                if len(completed_urls) == len(urls): #all done, trigger callback
                    return completed_callback(completed_urls)
                else:
                    continue
            assert url.startswith('http://') or url.startswith('https://')
            pool.spawn(make_stream_request, url)
        pool.join()

    if not isinstance(urls, (list, tuple)):
        urls = [urls,]
        
    urls = list(set(urls)) #remove duplicates (so we only fetch any given URL, once)
    groups = grouper(urls_group_size, urls)
    for i in xrange(len(groups)):
        #logging.debug("Stream fetching group %i of %i..." % (i, len(groups)))
        group = groups[i]
        if urls_group_time_spacing and i != 0:
            gevent.spawn_later(urls_group_time_spacing * i, process_group, group)
            #^ can leave to overlapping if not careful
        else:
            process_group(group) #should 'block' until each group processing is complete

def fetch_image(url, folder, filename, max_size=20*1024, formats=['png'], dimensions=(48, 48), fetch_timeout=1):
    def make_data_dir(subfolder):
        path = os.path.join(config.DATA_DIR, subfolder)
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    
    try:
        #fetch the image data 
        try:
            u = URL(url)
            client_kwargs = {'connection_timeout': fetch_timeout, 'network_timeout': fetch_timeout, 'insecure': True}
            if u.scheme == "https": client_kwargs['ssl_options'] = {'cert_reqs': gevent.ssl.CERT_NONE}
            client = HTTPClient.from_url(u, **client_kwargs)
            r = client.get(u.request_uri, headers={'Connection':'close'})
            raw_image_data = r.read(max_size) #read up to max_size
        except Exception, e:
            raise Exception("Got fetch_image request error: %s" % e)
        else:
            if r.status_code != 200:
                raise Exception("Bad status code returned from fetch_image: '%s'" % (r.status_code))
        finally:
            client.close()
    
        #decode image data
        try:
            image = Image.open(StringIO.StringIO(raw_image_data))
        except Exception, e:
            raise Exception("Unable to parse image data at: %s" % url)
        if image.format.lower() not in formats: raise Exception("Image is not a PNG: %s (got %s)" % (url, image.format))
        if image.size != dimensions: raise Exception("Image size is not 48x48: %s (got %s)" % (url, image.size))
        if image.mode not in ['RGB', 'RGBA']: raise Exception("Image mode is not RGB/RGBA: %s (got %s)" % (url, image.mode))
        imagePath = make_data_dir(folder)
        imagePath = os.path.join(imagePath, filename + '.' + image.format.lower())
        image.save(imagePath)
        os.system("exiftool -q -overwrite_original -all= %s" % imagePath) #strip all metadata, just in case
        return True
    except Exception, e:
        logging.warn(e)
        return False

def date_param(strDate):
    try:
        return calendar.timegm(dateutil.parser.parse(strDate).utctimetuple())
    except Exception, e:
        return False

def parse_iso8601_interval(value):
    try:
        return aniso8601.parse_interval(value)
    except Exception:
        try:
            return aniso8601.parse_repeating_interval(value)
        except Exception:
            raise FormatError('{} is not an iso8601 interval'.format(value))

def is_valid_json(data, schema):
    checker = FormatChecker();
    # add the "interval" format
    checker.checks("interval")(parse_iso8601_interval)
    validator = Draft4Validator(schema, format_checker=checker)
    errors = []
    for error in validator.iter_errors(data):
        errors.append(error.message)
    return errors

def next_interval_date(interval):
    try:
        generator = parse_iso8601_interval(interval)
    except Exception, e:
        return None

    def ts(dt):
        return time.mktime(dt.timetuple())

    previous = None
    next = generator.next()
    now = datetime.datetime.now()
    while ts(next) < ts(now) and next != previous:
        try:
            previous = next
            next = generator.next()
        except Exception, e:
            break

    if ts(next) < ts(now):
        return None
    else:
        return next.isoformat()

def subprocess_cmd(command):
    process = subprocess.Popen(command,stdout=subprocess.PIPE, shell=True)
    proc_stdout = process.communicate()[0].strip()
    print proc_stdout

def download_geoip_data():
    logging.info("Checking/updating GeoIP.dat ...")

    download = False;
    data_path = os.path.join(config.DATA_DIR, 'GeoIP.dat')
    if not os.path.isfile(data_path):
        download = True
    else:
        one_week_ago = time.time() - 60*60*24*7
        file_stat = os.stat(data_path)
        if file_stat.st_ctime < one_week_ago:
            download = True

    if download:
        logging.info("Downloading GeoIP.dat")
        cmd = "cd {}; wget -N -q http://geolite.maxmind.com/download/geoip/database/GeoLiteCountry/GeoIP.dat.gz; gzip -dfq GeoIP.dat.gz".format(config.DATA_DIR)
        subprocess_cmd(cmd)
    else:
        logging.info("GeoIP.dat OK")

def init_geoip():
    download_geoip_data();
    return pygeoip.GeoIP(os.path.join(config.DATA_DIR, 'GeoIP.dat'))

def block_cache(func):
    def cached_function(*args, **kwargs):
        
        function_signature = hashlib.sha256(func.__name__ + str(args) + str(kwargs)).hexdigest()

        sql = "SELECT block_index FROM blocks ORDER BY block_index DESC LIMIT 1"
        block_index = call_jsonrpc_api('sql', {'query': sql, 'bindings': []})['result'][0]['block_index']

        cached_result = config.mongo_db.counterblockd_cache.find_one({'block_index': block_index, 'function': function_signature})

        if not cached_result or config.TESTNET:
            #logging.info("generate cache ({}, {}, {})".format(func.__name__, block_index, function_signature))
            try:
                result = func(*args, **kwargs)
                config.mongo_db.counterblockd_cache.insert({
                    'block_index': block_index, 
                    'function': function_signature,
                    'result': json.dumps(result)
                })
                return result
            except Exception, e:
                logging.exception(e)
        else:
            #logging.info("result from cache ({}, {}, {})".format(func.__name__, block_index, function_signature))
            result = json.loads(cached_result['result'])
            return result
            
    return cached_function


def clean_block_cache(block_index):
    #logging.info("clean block cache lower than {}".format(block_index))
    config.mongo_db.counterblockd_cache.remove({'block_index': {'$lt': block_index}})

