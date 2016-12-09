import os
import re
import json
import base64
import logging
import datetime
import time
import decimal
import cgi
import itertools
import io
import subprocess
import calendar
import hashlib
import socket

import dateutil.parser
import gevent
import gevent.pool
import gevent.ssl
import grequests
import pymongo
import lxml.html
from PIL import Image
from jsonschema import FormatChecker, Draft4Validator, FormatError
import strict_rfc3339
import rfc3987
import aniso8601  # not needed here but to ensure that installed

from counterblock.lib import config

JSONRPC_API_REQUEST_TIMEOUT = 100  # in seconds

D = decimal.Decimal
logger = logging.getLogger(__name__)


def sanitize_eliteness(text):
    # strip out html data to avoid XSS-vectors
    return cgi.escape(lxml.html.document_fromstring(text).text_content())
    #^ wrap in cgi.escape - see https://github.com/mitotic/graphterm/issues/5


def http_basic_auth_str(username, password):
    """Returns a Basic Auth string."""
    authstr = 'Basic ' + base64.b64encode(('%s:%s' % (username, password)).encode('latin1')).decode("utf-8").strip()
    return authstr


def is_valid_url(url, suffix='', allow_localhost=False, allow_no_protocol=False):
    if url is None:
        return False

    regex = re.compile(
        r'^https?://' if not allow_no_protocol else r'^(https?://)?'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
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


def jsonrpc_api(method, params=None, endpoint=None, auth=None, abort_on_error=False, max_retry=10, retry_interval=3):
    retry = 0
    while retry < max_retry or max_retry == 0:
        try:
            result = call_jsonrpc_api(method, params=params, endpoint=endpoint, auth=auth, abort_on_error=abort_on_error)
            if 'result' not in result:
                raise AssertionError("Could not contact counterpartyd")
            return result
        except Exception as e:
            retry += 1
            logger.warn(str(e) + " -- Waiting {} seconds before trying again...".format(retry_interval))
            time.sleep(retry_interval)
            continue


def call_jsonrpc_api(method, params=None, endpoint=None, auth=None, abort_on_error=False):
    if not endpoint:
        endpoint = config.COUNTERPARTY_RPC
    if not auth:
        auth = config.COUNTERPARTY_AUTH
    if not params:
        params = {}

    payload = {
        "id": 0,
        "jsonrpc": "2.0",
        "method": method
    }
    if params:
        payload['params'] = params

    headers = {
        'Content-Type': 'application/json',
        'Connection': 'close',  # no keepalive
    }
    if auth:  # auth should be a (username, password) tuple, if specified
        headers['Authorization'] = http_basic_auth_str(auth[0], auth[1])

    try:
        r = grequests.map((grequests.post(endpoint, data=json.dumps(payload), timeout=JSONRPC_API_REQUEST_TIMEOUT, headers=headers),))
        r = r[0]
        if r is None:
            raise Exception("result is None")
    except Exception as e:
        raise Exception("Got call_jsonrpc_api request error: %s" % e)
    else:
        if r.status_code != 200:
            if abort_on_error:
                raise Exception("Bad status code returned: '%s'. result body: '%s'." % (r.status_code, r.text))
            else:
                logging.warning("Bad status code returned: '%s'. result body: '%s'." % (r.status_code, r.text))
                result = None
        else:
            result = r.json()

    if abort_on_error and 'error' in result and result['error'] is not None:
        raise Exception("Got back error from server: %s" % result['error'])

    return result


def get_url(url, abort_on_error=False, is_json=True, fetch_timeout=5, auth=None, post_data=None):
    """
    @param post_data: If not None, do a POST request, with the passed data (which should be in the correct string format already)
    """
    headers = {'Connection': 'close', }  # no keepalive
    if auth:
        # auth should be a (username, password) tuple, if specified
        headers['Authorization'] = http_basic_auth_str(auth[0], auth[1])

    try:
        if post_data is not None:
            if is_json:
                headers['content-type'] = 'application/json'
            r = grequests.map((grequests.post(url, data=post_data, timeout=fetch_timeout, headers=headers, verify=False),))[0]
        else:
            r = grequests.map((grequests.get(url, timeout=fetch_timeout, headers=headers, verify=False),))[0]
        if r is None:
            raise Exception("result is None")
    except Exception as e:
        raise Exception("Got get_url request error: %s" % e)
    else:
        if r.status_code != 200 and abort_on_error:
            raise Exception("Bad status code returned: '%s'. result body: '%s'." % (r.status_code, r.text))
    return r.json() if r.text and is_json else r.text


def grouper(n, iterable, fillmissing=False, fillvalue=None):
    # Modified from http://stackoverflow.com/a/1625013
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    data = itertools.zip_longest(*args, fillvalue=fillvalue)
    if not fillmissing:
        data = [[e for e in g if e != fillvalue] for g in data]
    return data


def cmp(a, b):
    """replaces the missing built-in in Python3"""
    if a is None and b is None:
        return 0
    elif a is None:
        return -1
    elif b is None:
        return 1
    else:
        return (a > b) - (a < b)


def multikeysort(items, columns):
    """http://stackoverflow.com/a/1144405"""
    from operator import itemgetter as i
    from functools import cmp_to_key
    comparers = [
        ((i(col[1:].strip()), -1) if col.startswith('-') else (i(col.strip()), 1))
        for col in columns
    ]
    def comparer(left, right):
        comparer_iter = (
            cmp(fn(left), fn(right)) * mult
            for fn, mult in comparers
        )
        return next((result for result in comparer_iter if result), 0)
    return sorted(items, key=cmp_to_key(comparer))


def cumsum(iterable):
    values = list(iterable)
    for pos in range(1, len(values)):
        values[pos] += values[pos - 1]
    return values


def moving_average(samples, n=3):
    ret = cumsum(samples)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n - 1:] / n


def weighted_average(value_weight_list):
    """Takes a list of tuples (value, weight) and returns weighted average as
    calculated by Sum of all values * weights / Sum of all weights
    http://bcdcspatial.blogspot.com/2010/08/simple-weighted-average-with-python.html
    """
    numerator = sum([v * w for v, w in value_weight_list])
    denominator = sum([w for v, w in value_weight_list])
    if(denominator != 0):
        return(float(numerator) / float(denominator))
    else:
        return None


def json_dthandler(obj):
    #if bytes, convert to string
    if isinstance(obj, bytes):
        obj = str(obj, 'utf-8')

    if hasattr(obj, 'timetuple'):  # datetime object
        # give datetime objects to javascript as epoch ts in ms (i.e. * 1000)
        return int(calendar.timegm(obj.timetuple())) * 1000
    else:
        raise TypeError('Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj)))


def stream_fetch(urls, completed_callback, urls_group_size=50, urls_group_time_spacing=0, max_fetch_size=4 * 1024,
                 fetch_timeout=1, is_json=True, per_request_complete_callback=None):
    completed_urls = {}

    def make_stream_request(url):
        try:
            r = grequests.map((grequests.get(url, timeout=fetch_timeout, headers={'Connection': 'close'}, verify=False, stream=True),))[0]
            if r is None:
                raise Exception("result is None")
        except Exception as e:
            data = (False, "Got exception: %s" % e)
        else:
            if r.status_code != 200:
                data = (False, "Got non-successful response code of: %s" % r.status_code)
            else:
                try:
                    # read up to max_fetch_size
                    raw_data = next(r.iter_content(chunk_size=max_fetch_size))
                    if is_json:  # try to convert to JSON
                        try:
                            data = json.loads(raw_data.decode('utf-8'))
                        except Exception as e:
                            data = (False, "Invalid JSON data: %s" % e)
                        else:
                            data = (True, data)
                    else:  # keep raw
                        data = (True, raw_data)
                except Exception as e:
                    data = (False, "Request error: %s" % e)
        finally:
            if r:
                r.close()

        if per_request_complete_callback:
            per_request_complete_callback(url, data)

        completed_urls[url] = data
        if len(completed_urls) == len(urls):  # all done, trigger callback
            return completed_callback(completed_urls)

    def process_group(group):
        group_results = []
        pool = gevent.pool.Pool(urls_group_size)
        for url in group:
            if not is_valid_url(url, allow_no_protocol=True):
                completed_urls[url] = (False, "Invalid URL")
                if len(completed_urls) == len(urls):  # all done, trigger callback
                    return completed_callback(completed_urls)
                else:
                    continue
            assert url.startswith('http://') or url.startswith('https://')
            pool.spawn(make_stream_request, url)
        pool.join()

    if not isinstance(urls, (list, tuple)):
        urls = [urls, ]

    urls = list(set(urls))  # remove duplicates (so we only fetch any given URL, once)
    groups = grouper(urls_group_size, urls)
    for i in range(len(groups)):
        #logger.debug("Stream fetching group %i of %i..." % (i, len(groups)))
        group = groups[i]
        if urls_group_time_spacing and i != 0:
            gevent.spawn_later(urls_group_time_spacing * i, process_group, group)
            #^ can leave to overlapping if not careful
        else:
            process_group(group)  # should 'block' until each group processing is complete


def fetch_image(url, folder, filename, max_size=20 * 1024, formats=['png'], dimensions=(48, 48), fetch_timeout=1):
    def make_data_dir(subfolder):
        path = os.path.join(config.data_dir, subfolder)
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    try:
        # fetch the image data
        try:
            r = grequests.map((grequests.get(url, timeout=fetch_timeout, headers={'Connection': 'close'}, verify=False, stream=True),))[0]
            if r is None:
                raise Exception("result is None")

            raw_image_data = r.iter_content(chunk_size=max_size)  # read up to max_size
        except Exception as e:
            raise Exception("Got fetch_image request error: %s" % e)
        else:
            if r.status_code != 200:
                raise Exception("Bad status code returned from fetch_image: '%s'" % (r.status_code))
        finally:
            if r:
                r.close()

        # decode image data
        try:
            image = Image.open(io.StringIO(raw_image_data))
        except Exception as e:
            raise Exception("Unable to parse image data at: %s" % url)
        if image.format.lower() not in formats:
            raise Exception("Image is not a PNG: %s (got %s)" % (url, image.format))
        if image.size != dimensions:
            raise Exception("Image size is not 48x48: %s (got %s)" % (url, image.size))
        if image.mode not in ['RGB', 'RGBA']:
            raise Exception("Image mode is not RGB/RGBA: %s (got %s)" % (url, image.mode))
        imagePath = make_data_dir(folder)
        imagePath = os.path.join(imagePath, filename + '.' + image.format.lower())
        image.save(imagePath)
        os.system("exiftool -q -overwrite_original -all= %s" % imagePath)  # strip all metadata, just in case
        return True
    except Exception as e:
        logger.warn(e)
        return False


def date_param(strDate):
    try:
        return calendar.timegm(dateutil.parser.parse(strDate).utctimetuple())
    except Exception as e:
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
    checker = FormatChecker()
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
    except Exception as e:
        return None

    def ts(dt):
        return calendar.timegm(dt.timetuple())

    previous = None
    next = next(generator)
    now = datetime.datetime.utcnow()
    while ts(next) < ts(now) and next != previous:
        try:
            previous = next
            next = next(generator)
        except Exception as e:
            break

    if ts(next) < ts(now):
        return None
    else:
        return next.isoformat()


def subprocess_cmd(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    proc_stdout = process.communicate()[0].strip()
    print(proc_stdout)
