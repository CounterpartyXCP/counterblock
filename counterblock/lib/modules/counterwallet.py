"""
Implements counterwallet support as a counterblock plugin

Python 2.x, as counterblock is still python 2.x
"""
import os
import sys
import time
import datetime
import logging
import decimal
import urllib.request
import urllib.parse
import urllib.error
import json
import pymongo
import flask
import jsonrpc
import configparser
import calendar

import dateutil.parser

from counterblock.lib import config, util, blockfeed, blockchain, messages
from counterblock.lib.processor import MessageProcessor, MempoolMessageProcessor, BlockProcessor, StartUpProcessor, CaughtUpProcessor, RollbackProcessor, API, start_task, CORE_FIRST_PRIORITY
from counterblock.lib.modules import CWALLET_PRIORITY_PARSE_FOR_SOCKETIO, CWALLET_PRIORITY_PUBLISH_MEMPOOL

from counterblock.lib.processor import startup

PREFERENCES_MAX_LENGTH = 100000  # in bytes, as expressed in JSON
ARMORY_UTXSVR_PORT_MAINNET = 6590
ARMORY_UTXSVR_PORT_TESTNET = 6591
ARMORY_UTXSVR_PORT_REGTEST = 6592

FUZZY_MAX_WALLET_MESSAGES_STORED = 1000

D = decimal.Decimal
logger = logging.getLogger(__name__)
module_config = {}


def _read_config():
    configfile = configparser.SafeConfigParser(
        defaults=os.environ, allow_no_value=True, inline_comment_prefixes=('#', ';'))
    config_path = os.path.join(config.config_dir, 'counterwallet%s.conf' % config.net_path_part)
    logger.info("Loading config at: %s" % config_path)
    try:
        configfile.read(config_path)
        assert configfile.has_section('Default')
    except:
        logging.warn("Could not find or parse counterwallet%s.conf config file!" % config.net_path_part)

    # armory-utxsvr
    if configfile.has_option('Default', 'armory-utxsvr-host'):
        module_config['ARMORY_UTXSVR_HOST'] = configfile.get('Default', 'armory-utxsvr-host')
    else:
        module_config['ARMORY_UTXSVR_HOST'] = "127.0.0.1"

    # email-related
    if configfile.has_option('Default', 'support-email'):
        module_config['SUPPORT_EMAIL'] = configfile.get('Default', 'support-email')
    else:
        module_config['SUPPORT_EMAIL'] = None  # disabled
    if module_config['SUPPORT_EMAIL']:
        if not email.utils.parseaddr(module_config['SUPPORT_EMAIL'])[1]:
            raise Exception("Invalid support email address")

    if configfile.has_option('Default', 'email-server'):
        module_config['EMAIL_SERVER'] = configfile.get('Default', 'email-server')
    else:
        module_config['EMAIL_SERVER'] = "localhost"

    # pref pruning
    if configfile.has_option('Default', 'prefs-prune-enable'):
        module_config['PREFS_PRUNE_ENABLE'] = configfile.getboolean('Default', 'prefs-prune-enable')
    else:
        module_config['PREFS_PRUNE_ENABLE'] = False

    # vending machine integration
    if configfile.has_option('Default', 'vending-machine-provider'):
        module_config['VENDING_MACHINE_PROVIDER'] = configfile.get('Default', 'vending-machine-provider')
    else:
        module_config['VENDING_MACHINE_PROVIDER'] = None


@API.add_method
def is_ready():
    """this method used by the client to check if the server is alive, caught up, and ready to accept requests.
    If the server is NOT caught up, a 525 error will be returned actually before hitting this point. Thus,
    if we actually return data from this function, it should always be true. (may change this behaviour later)"""

    ip = flask.request.headers.get('X-Real-Ip', flask.request.remote_addr)
    try:
        country = module_config['GEOIP'].city(ip).country.iso_code
    except Exception:
        country = "unknown"
    return {
        'caught_up': blockfeed.fuzzy_is_caught_up(),
        'last_message_index': config.state['last_message_index'],
        'cw_last_message_seq': config.state['cw_last_message_seq'],
        'block_height': config.state['cp_backend_block_index'],
        'testnet': config.TESTNET,
        'regtest': config.REGTEST,
        'ip': ip,
        'country': country,
        'quote_assets': config.QUOTE_ASSETS,
        'quick_buy_enable': True if module_config['VENDING_MACHINE_PROVIDER'] is not None else False
    }


@API.add_method
def get_reflected_host_info():
    """Allows the requesting host to get some info about itself, such as its IP. Used for troubleshooting."""
    ip = flask.request.headers.get('X-Real-Ip', flask.request.remote_addr)
    country = module_config['GEOIP'].city(ip).country.iso_code
    return {
        'ip': ip,
        'cookie': flask.request.headers.get('Cookie', ''),
        'country': country
    }


@API.add_method
def get_wallet_stats(start_ts=None, end_ts=None):
    now_ts = calendar.timegm(time.gmtime())
    if not end_ts:  # default to current datetime
        end_ts = now_ts
    if not start_ts:  # default to 360 days before the end date
        start_ts = end_ts - (360 * 24 * 60 * 60)

    num_wallets_mainnet = config.mongo_db.preferences.find({'network': 'mainnet'}).count()
    num_wallets_testnet = config.mongo_db.preferences.find({'network': 'testnet'}).count()
    num_wallets_regtest = config.mongo_db.preferences.find({'network': 'regtest'}).count()
    num_wallets_unknown = config.mongo_db.preferences.find({'network': None}).count()
    wallet_stats = []

    for net in ['mainnet', 'testnet', 'regtest']:
        filters = {
            "when": {
                "$gte": datetime.datetime.utcfromtimestamp(start_ts)
            } if end_ts == now_ts else {
                "$gte": datetime.datetime.utcfromtimestamp(start_ts),
                "$lte": datetime.datetime.utcfromtimestamp(end_ts)
            },
            'network': net
        }
        stats = config.mongo_db.wallet_stats.find(filters).sort('when', pymongo.ASCENDING)
        new_wallet_counts = []
        login_counts = []
        distinct_login_counts = []
        for e in stats:
            d = int(calendar.timegm(datetime.datetime(e['when'].year, e['when'].month, e['when'].day).timetuple()) * 1000)

            if 'distinct_login_count' in e:
                distinct_login_counts.append([d, e['distinct_login_count']])
            if 'login_count' in e:
                login_counts.append([d, e['login_count']])
            if 'new_count' in e:
                new_wallet_counts.append([d, e['new_count']])

        wallet_stats.append({'name': '%s: Logins' % net.capitalize(), 'data': login_counts})
        wallet_stats.append({'name': '%s: Active Wallets' % net.capitalize(), 'data': distinct_login_counts})
        wallet_stats.append({'name': '%s: New Wallets' % net.capitalize(), 'data': new_wallet_counts})

    return {
        'num_wallets_mainnet': num_wallets_mainnet,
        'num_wallets_testnet': num_wallets_testnet,
        'num_wallets_regtest': num_wallets_regtest,
        'num_wallets_unknown': num_wallets_unknown,
        'wallet_stats': wallet_stats}


@API.add_method
def get_preferences(wallet_id, for_login=False, network=None):
    """Gets stored wallet preferences
    @param network: only required if for_login is specified. One of: 'mainnet', 'testnet' or 'regtest'
    """
    if network not in (None, 'mainnet', 'testnet', 'regtest'):
        raise Exception("Invalid network parameter setting")
    if for_login and network is None:
        raise Exception("network parameter required if for_login is set")

    result = config.mongo_db.preferences.find_one({"wallet_id": wallet_id})
    if not result:
        return False  # doesn't exist

    last_touched_date = datetime.datetime.utcfromtimestamp(result['last_touched']).date()
    now = datetime.datetime.utcnow()

    if for_login:  # record user login
        ip = flask.request.headers.get('X-Real-Ip', flask.request.remote_addr)
        ua = flask.request.headers.get('User-Agent', '')
        config.mongo_db.login_history.insert({'wallet_id': wallet_id, 'when': now, 'network': network, 'action': 'login', 'ip': ip, 'ua': ua})

    result['last_touched'] = calendar.timegm(time.gmtime())
    config.mongo_db.preferences.save(result)

    return {
        'preferences': json.loads(result['preferences']),
        'last_updated': result.get('last_updated', None)
    }


@API.add_method
def store_preferences(wallet_id, preferences, for_login=False, network=None, referer=None):
    """Stores freeform wallet preferences
    @param network: only required if for_login is specified. One of: 'mainnet', 'testnet' or 'regtest'
    """
    if network not in (None, 'mainnet', 'testnet', 'regtest'):
        raise Exception("Invalid network parameter setting")
    if for_login and network is None:
        raise Exception("network parameter required if for_login is set")
    if not isinstance(preferences, dict):
        raise Exception("Invalid preferences object")
    try:
        preferences_json = json.dumps(preferences)
    except:
        raise Exception("Cannot dump preferences to JSON")

    now = datetime.datetime.utcnow()

    # sanity check around max size
    if len(preferences_json) >= PREFERENCES_MAX_LENGTH:
        raise Exception("Preferences object is too big.")

    if for_login:  # mark this as a new signup IF the wallet doesn't exist already
        existing_record = config.mongo_db.login_history.find({'wallet_id': wallet_id, 'network': network, 'action': 'create'})
        if existing_record.count() == 0:
            ip = flask.request.headers.get('X-Real-Ip', flask.request.remote_addr)
            ua = flask.request.headers.get('User-Agent', '')
            config.mongo_db.login_history.insert(
                {'wallet_id': wallet_id, 'when': now,
                 'network': network, 'action': 'create', 'referer': referer, 'ip': ip, 'ua': ua})
            config.mongo_db.login_history.insert(
                {'wallet_id': wallet_id, 'when': now,
                 'network': network, 'action': 'login', 'ip': ip, 'ua': ua})  # also log a wallet login

    now_ts = calendar.timegm(time.gmtime())
    config.mongo_db.preferences.update(
        {'wallet_id': wallet_id},
        {'$set': {
            'wallet_id': wallet_id,
            'preferences': preferences_json,
            'last_updated': now_ts,
            'last_touched': now_ts},
         '$setOnInsert': {'when_created': now_ts, 'network': network}
         }, upsert=True)
    #^ last_updated MUST be in GMT, as it will be compaired again other servers
    return True


@API.add_method
def create_armory_utx(unsigned_tx_hex, public_key_hex):
    port = ARMORY_UTXSVR_PORT_MAINNET
    if config.TESTNET:
        port = ARMORY_UTXSVR_PORT_TESTNET
    elif config.REGTEST:
        port = ARMORY_UTXSVR_PORT_REGTEST
    endpoint = "http://%s:%s/" % (module_config['ARMORY_UTXSVR_HOST'], port)
    params = {'unsigned_tx_hex': unsigned_tx_hex, 'public_key_hex': public_key_hex}
    utx_ascii = util.call_jsonrpc_api("serialize_unsigned_tx", params=params, endpoint=endpoint, abort_on_error=True)['result']
    return utx_ascii


@API.add_method
def convert_armory_signedtx_to_raw_hex(signed_tx_ascii):
    port = ARMORY_UTXSVR_PORT_MAINNET
    if config.TESTNET:
        port = ARMORY_UTXSVR_PORT_TESTNET
    elif config.REGTEST:
        port = ARMORY_UTXSVR_PORT_REGTEST
    endpoint = "http://%s:%s/" % (module_config['ARMORY_UTXSVR_HOST'], port)
    params = {'signed_tx_ascii': signed_tx_ascii}
    raw_tx_hex = util.call_jsonrpc_api("convert_signed_tx_to_raw_hex", params=params, endpoint=endpoint, abort_on_error=True)['result']
    return raw_tx_hex


@API.add_method
def create_support_case(name, from_email, problem, screenshot=None, addtl_info=''):
    """create an email with the information received
    @param screenshot: The base64 text of the screenshot itself, prefixed with data=image/png ...,
    @param addtl_info: A JSON-encoded string of a dict with additional information to include in the support request
    """
    import smtplib
    import email.utils
    from email.header import Header
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEBase import MIMEBase
    from email.MIMEText import MIMEText
    from email.mime.image import MIMEImage

    if not module_config['SUPPORT_EMAIL']:
        raise Exception("Sending of support emails are disabled on the server: no SUPPORT_EMAIL address set")

    if not email.utils.parseaddr(from_email)[1]:  # should have been validated in the form
        raise Exception("Invalid support email address")

    try:
        if screenshot:
            screenshot_data = screenshot.split(',', 1)[1]
            screenshot_data_decoded = base64.b64decode(screenshot_data)
    except:
        raise Exception("screenshot data format unexpected")

    try:
        addtl_info = json.loads(addtl_info)
        addtl_info = json.dumps(addtl_info, indent=1, sort_keys=False)
    except:
        raise Exception("addtl_info data format unexpected")

    from_email_formatted = email.utils.formataddr((name, from_email))
    msg = MIMEMultipart()
    msg['Subject'] = Header((problem[:75] + '...') if len(problem) > 75 else problem, 'utf-8')
    msg['From'] = from_email_formatted
    msg['Reply-to'] = from_email_formatted
    msg['To'] = module_config['SUPPORT_EMAIL']
    msg['Date'] = email.utils.formatdate(localtime=True)

    msg_text = MIMEText("""Problem: %s\n\nAdditional Info:\n%s""" % (problem, addtl_info))
    msg.attach(msg_text)

    if screenshot:
        image = MIMEImage(screenshot_data_decoded, name="screenshot.png")
        msg.attach(image)

    server = smtplib.SMTP(module_config['EMAIL_SERVER'])
    server.sendmail(from_email, module_config['SUPPORT_EMAIL'], msg.as_string())
    return True


@API.add_method
def get_vennd_machine():
    if module_config['VENDING_MACHINE_PROVIDER'] is not None:
        return util.get_url(module_config['VENDING_MACHINE_PROVIDER'])
    else:
        return []


@API.add_method
def get_latest_wallet_messages(last_seq):
    if last_seq is None:
        last_seq = -1  # 0 and above
    results = list(config.mongo_db.wallet_messages.find({"_id": {"$gt": last_seq}}, sort=[("_id", pymongo.ASCENDING)]))
    logger.debug("get_latest_wallet_messages: last seq {}, returning {} entries".format(last_seq, len(results)))
    return results


def task_expire_stale_prefs():
    """
    Every day, clear out preferences objects that haven't been touched in > 30 days, in order to reduce abuse risk/space consumed
    """
    min_last_updated = calendar.timegm((datetime.datetime.utcnow() - datetime.timedelta(days=30)).timetuple())

    num_stale_records = config.mongo_db.preferences.find({'last_touched': {'$lt': min_last_updated}}).count()
    config.mongo_db.preferences.remove({'last_touched': {'$lt': min_last_updated}})
    if num_stale_records:
        logger.warn("REMOVED %i stale preferences objects" % num_stale_records)

    start_task(task_expire_stale_prefs, delay=86400)  # call again in 1 day


def task_generate_wallet_stats():
    """
    Every 30 minutes, from the login history, update and generate wallet stats
    """
    def gen_stats_for_network(network):
        assert network in ('mainnet', 'testnet', 'regtest')
        # get the latest date in the stats table present
        now = datetime.datetime.utcnow()
        latest_stat = config.mongo_db.wallet_stats.find({'network': network}).sort('when', pymongo.DESCENDING).limit(1)
        latest_stat = latest_stat[0] if latest_stat.count() else None
        new_entries = {}

        # the queries below work with data that happened on or after the date of the latest stat present
        # aggregate over the same period for new logins, adding the referrers to a set
        match_criteria = {'when': {"$gte": latest_stat['when']}, 'network': network, 'action': 'create'} \
            if latest_stat else {'when': {"$lte": now}, 'network': network, 'action': 'create'}
        new_wallets = config.mongo_db.login_history.aggregate([
            {"$match": match_criteria},
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
        for e in new_wallets:
            ts = calendar.timegm(datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']).timetuple())
            new_entries[ts] = {  # a future wallet_stats entry
                'when': datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']),
                'network': network,
                'new_count': e['new_count'],
            }

        referer_counts = config.mongo_db.login_history.aggregate([
            {"$match": match_criteria},
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
        for e in referer_counts:
            ts = calendar.timegm(datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']).timetuple())
            assert ts in new_entries
            if e['_id']['referer'] is None:
                continue
            referer_key = urllib.parse.quote(e['_id']['referer']).replace('.', '%2E')
            if 'referers' not in new_entries[ts]:
                new_entries[ts]['referers'] = {}
            if e['_id']['referer'] not in new_entries[ts]['referers']:
                new_entries[ts]['referers'][referer_key] = 0
            new_entries[ts]['referers'][referer_key] += 1

        # logins (not new wallets) - generate stats
        match_criteria = {'when': {"$gte": latest_stat['when']}, 'network': network, 'action': 'login'} \
            if latest_stat else {'when': {"$lte": now}, 'network': network, 'action': 'login'}
        logins = config.mongo_db.login_history.aggregate([
            {"$match": match_criteria},
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
        for e in logins:
            ts = calendar.timegm(datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']).timetuple())
            if ts not in new_entries:
                new_entries[ts] = {  # a future wallet_stats entry
                    'when': datetime.datetime(e['_id']['year'], e['_id']['month'], e['_id']['day']),
                    'network': network,
                    'new_count': 0,
                    'referers': []
                }
            new_entries[ts]['login_count'] = e['login_count']
            new_entries[ts]['distinct_login_count'] = len(e['distinct_wallets'])

        # add/replace the wallet_stats data
        if latest_stat:
            updated_entry_ts = calendar.timegm(datetime.datetime(
                latest_stat['when'].year, latest_stat['when'].month, latest_stat['when'].day).timetuple())
            if updated_entry_ts in new_entries:
                updated_entry = new_entries[updated_entry_ts]
                del new_entries[updated_entry_ts]
                assert updated_entry['when'] == latest_stat['when']
                del updated_entry['when']  # not required for the upsert
                logger.info(
                    "Revised wallet statistics for partial day %s-%s-%s: %s"
                    % (latest_stat['when'].year, latest_stat['when'].month, latest_stat['when'].day, updated_entry))
                config.mongo_db.wallet_stats.update(
                    {'when': latest_stat['when']},
                    {"$set": updated_entry}, upsert=True)

        if new_entries:  # insert the rest
            #logger.info("Stats, new entries: %s" % new_entries.values())
            config.mongo_db.wallet_stats.insert(list(new_entries.values()))
            logger.info("Added wallet statistics for %i full days" % len(list(new_entries.values())))

    gen_stats_for_network('mainnet')
    gen_stats_for_network('testnet')
    gen_stats_for_network('regtest')
    start_task(task_generate_wallet_stats, delay=30 * 60)  # call again in 30 minutes


def store_wallet_message(msg, msg_data, decorate=True):
    wallet_message = messages.decorate_message_for_feed(msg, msg_data=msg_data) if decorate else msg

    # use "optimistic loop" pattern to insert a new messages with an incrementing seq
    while True:
        last_seq = config.mongo_db.wallet_messages.find_one(sort=[("_id", pymongo.DESCENDING)])['_id']
        new_seq = last_seq + 1
        try:
            config.mongo_db.wallet_messages.insert({
                '_id': new_seq,
                'when': calendar.timegm(time.gmtime()),
                'message': wallet_message,
            })
        except pymongo.errors.DuplicateKeyError:
            continue
        else:
            logger.debug("store_wallet_message: stored {}".format(new_seq))
            if config.state['cw_last_message_seq'] < new_seq:
                config.state['cw_last_message_seq'] = new_seq
            break

    # every so often, trim up the table
    if new_seq % 20 == 0:  # for performance, don't do this every iteration
        if config.mongo_db.wallet_messages.count() > FUZZY_MAX_WALLET_MESSAGES_STORED:
            config.mongo_db.wallet_messages.remove({'_id': {'$lte': new_seq - FUZZY_MAX_WALLET_MESSAGES_STORED}})


@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 0.5)
def handle_invalid(msg, msg_data):
    # don't process invalid messages, but do forward them along to clients
    pre_status = msg_data.get('status', 'valid')
    if type(pre_status) == str:
        status = msg_data.get('status', 'valid').lower()
    else:
        status = str(pre_status)
    if status.startswith('invalid'):
        if config.state['cp_latest_block_index'] - config.state['my_latest_block']['block_index'] < config.MAX_REORG_NUM_BLOCKS:
            # forward along via message feed, except while we're catching up
            store_wallet_message(msg, msg_data)
        config.state['last_message_index'] = msg['message_index']
        return 'ABORT_THIS_MESSAGE_PROCESSING'


@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 0.9)  # should run BEFORE processor.messages.handle_reorg()
def handle_reorg(msg, msg_data):
    if msg['command'] == 'reorg':
       # send out the message to listening clients (but don't forward along while we're catching up)
        if config.state['cp_latest_block_index'] - config.state['my_latest_block']['block_index'] < config.MAX_REORG_NUM_BLOCKS:
            msg_data['_last_message_index'] = config.state['last_message_index']
            store_wallet_message(msg, msg_data)
            event = messages.decorate_message_for_feed(msg, msg_data=msg_data)
        # processor.messages.handle_reorg() will run immediately after this and handle the rest


@MessageProcessor.subscribe(priority=CWALLET_PRIORITY_PARSE_FOR_SOCKETIO)
def store_new_messages(msg, msg_data):
    # if we're catching up beyond MAX_REORG_NUM_BLOCKS blocks out, make sure not to send out any socket.io
    # events, as to not flood on a resync (as we may give a 525 to kick the logged in clients out, but we
    # can't guarantee that the socket.io connection will always be severed as well??)
    if config.state['cp_latest_block_index'] - config.state['my_latest_block']['block_index'] < config.MAX_REORG_NUM_BLOCKS:
        # send out the message to listening clients
        store_wallet_message(msg, msg_data)


@MempoolMessageProcessor.subscribe(priority=CWALLET_PRIORITY_PUBLISH_MEMPOOL)
def publish_mempool_tx(msg, msg_data):
    store_wallet_message(msg, msg_data, decorate=False)


@CaughtUpProcessor.subscribe()
def start_tasks():
    start_task(task_expire_stale_prefs)
    start_task(task_generate_wallet_stats)


@StartUpProcessor.subscribe()
def init():
    _read_config()
    logger.info("Armory-utxsvr hostname: {}".format(module_config['ARMORY_UTXSVR_HOST']))

    # init db and indexes
    # COLLECTIONS THAT *ARE* PURGED AS A RESULT OF A REPARSE
    #wallet_messages
    #config.mongo_db.wallet_messages.ensure_index('when')
    # wallet_stats
    config.mongo_db.wallet_stats.ensure_index([
        ("when", pymongo.ASCENDING),
        ("network", pymongo.ASCENDING),
    ])

    # COLLECTIONS THAT ARE *NOT* PURGED AS A RESULT OF A REPARSE
    # preferences
    config.mongo_db.preferences.ensure_index('wallet_id', unique=True)
    config.mongo_db.preferences.ensure_index('network')
    config.mongo_db.preferences.ensure_index('last_touched')
    # login_history
    config.mongo_db.login_history.ensure_index('wallet_id')
    config.mongo_db.login_history.ensure_index([
        ("when", pymongo.DESCENDING),
        ("network", pymongo.ASCENDING),
        ("action", pymongo.ASCENDING),
    ])

    # clear the wallet_messages collection, but create a null entry with the last message ID (because it could
    # have been a rapid restart and we don't want to break wallets currently pulling for messages)
    last_wallet_message = config.mongo_db.wallet_messages.find_one(sort=[("_id", pymongo.DESCENDING)])
    if not last_wallet_message:
        config.mongo_db.wallet_messages.insert({
            '_id': 0,
            'when': calendar.timegm(time.gmtime()),
            'message': None,
        })
    config.state['cw_last_message_seq'] = last_wallet_message['_id'] if last_wallet_message else 0
    logger.debug("cw_last_message_seq: {}".format(config.state['cw_last_message_seq']))

    # init GEOIP
    import geoip2.database
    mmdbName = 'GeoLite2-City.mmdb'
    geoip_data_path = os.path.join(config.data_dir, mmdbName)


    def download_geoip_data():
        logger.info("Checking/updating {} ...".format(mmdbName))
        download = False

        if not os.path.isfile(geoip_data_path):
            download = True
        else:
            one_week_ago = time.time() - 60 * 60 * 24 * 7
            file_stat = os.stat(geoip_data_path)
            if file_stat.st_ctime < one_week_ago:
                download = True

        if download:
            logger.info("Downloading {}".format(mmdbName))
            # TODO: replace with pythonic way to do this!
            cmd = "cd '{}' && wget -N -q https://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz && tar xzf GeoLite2-City.tar.gz && cp */GeoLite2-City.mmdb .".format(config.data_dir)
            util.subprocess_cmd(cmd)
        else:
            logger.info("{} database up to date. Not downloading.".format(mmdbName))

    download_geoip_data()
    try:
        module_config['GEOIP'] = geoip2.database.Reader(geoip_data_path)
    except FileNotFoundError as e:
        logger.warn("GeoLite2-City.mmdb not found, download from https://maxmind.com/ the GeoLite2-City and put in {}".format(geoip_data_path))

    if not module_config['SUPPORT_EMAIL']:
        logger.warn("Support email setting not set: To enable, please specify an email for the 'support-email' setting in your counterblockd.conf")


@RollbackProcessor.subscribe()
def process_rollback(max_block_index):
    if not max_block_index:  # full reparse
        config.mongo_db.wallet_stats.drop()

        #clear the wallet_messages collection, and create a null entry with a message ID of 0
        config.mongo_db.wallet_messages.drop()
        config.mongo_db.wallet_messages.insert({
            '_id': 0,
            'when': calendar.timegm(time.gmtime()),
            'message': None,
        })
        config.state['cw_last_message_seq'] = 0
    else:  # rollback
        pass
