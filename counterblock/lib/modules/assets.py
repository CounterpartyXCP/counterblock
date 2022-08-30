"""
Implements counterwallet asset-related support as a counterblock plugin

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
import base64
import pymongo
import configparser
import calendar

import dateutil.parser

from counterblock.lib import config, util, blockfeed, blockchain
from counterblock.lib.modules import ASSETS_PRIORITY_PARSE_ISSUANCE, ASSETS_PRIORITY_PARSE_DESTRUCTION, ASSETS_PRIORITY_BALANCE_CHANGE
from counterblock.lib.processor import MessageProcessor, MempoolMessageProcessor, BlockProcessor, StartUpProcessor, CaughtUpProcessor, RollbackProcessor, API, start_task

ASSET_MAX_RETRY = 3

D = decimal.Decimal
logger = logging.getLogger(__name__)


def inc_fetch_retry(asset, max_retry=ASSET_MAX_RETRY, new_status='error', errors=[]):
    asset['fetch_info_retry'] += 1
    asset['errors'] = errors
    if asset['fetch_info_retry'] == max_retry:
        asset['info_status'] = new_status
    config.mongo_db.asset_extended_info.save(asset)


def process_asset_info(asset, info_data):
    def sanitize_json_data(data):
        data['asset'] = util.sanitize_eliteness(data['asset'])
        if 'description' in data:
            data['description'] = util.sanitize_eliteness(data['description'])
        if 'website' in data:
            data['website'] = util.sanitize_eliteness(data['website'])
        if 'pgpsig' in data:
            data['pgpsig'] = util.sanitize_eliteness(data['pgpsig'])
        return data

    # sanity check
    assert asset['info_status'] == 'needfetch'
    assert 'info_url' in asset
    assert util.is_valid_url(asset['info_url'], allow_no_protocol=True)  # already validated in the fetch

    errors = util.is_valid_json(info_data, config.ASSET_SCHEMA)

    if not isinstance(info_data, dict) or 'asset' not in info_data:
        errors.append('Invalid data format')
    elif asset['asset'] != info_data['asset']:
        errors.append('asset field does not match asset name')

    if len(errors) > 0:
        inc_fetch_retry(asset, new_status='invalid', errors=errors)
        return (False, errors)

    asset['info_status'] = 'valid'

    # fetch any associated images...
    # TODO: parallelize this 2nd level asset image fetching ... (e.g. just compose a list here, and process it in later on)
    if 'image' in info_data:
        info_data['valid_image'] = util.fetch_image(
            info_data['image'], config.SUBDIR_ASSET_IMAGES, asset['asset'], fetch_timeout=5)

    asset['info_data'] = sanitize_json_data(info_data)
    config.mongo_db.asset_extended_info.save(asset)
    return (True, None)


def task_compile_extended_asset_info():
    assets = list(config.mongo_db.asset_extended_info.find({'info_status': 'needfetch'}))
    asset_info_urls = []

    def asset_fetch_complete_hook(urls_data):
        logger.info("Enhanced asset info fetching complete. %s unique URLs fetched. Processing..." % len(urls_data))
        for asset in assets:
            logger.debug("Looking at asset %s: %s" % (asset, asset['info_url']))
            if asset['info_url']:
                info_url = ('http://' + asset['info_url']) \
                    if not asset['info_url'].startswith('http://') and not asset['info_url'].startswith('https://') else asset['info_url']
                assert info_url in urls_data
                if not urls_data[info_url][0]:  # request was not successful
                    inc_fetch_retry(asset, max_retry=ASSET_MAX_RETRY, errors=[urls_data[info_url][1]])
                    logger.warn("Fetch for asset at %s not successful: %s (try %i of %i)" % (
                        info_url, urls_data[info_url][1], asset['fetch_info_retry'], ASSET_MAX_RETRY))
                else:
                    result = process_asset_info(asset, urls_data[info_url][1])
                    if not result[0]:
                        logger.info("Processing for asset %s at %s not successful: %s" % (asset['asset'], info_url, result[1]))
                    else:
                        logger.debug("Processing for asset %s at %s successful" % (asset['asset'], info_url))

    # compose and fetch all info URLs in all assets with them
    for asset in assets:
        if not asset['info_url']:
            continue

        if asset.get('disabled', False):
            logger.info("ExtendedAssetInfo: Skipping disabled asset %s" % asset['asset'])
            continue

        # may or may not end with .json. may or may not start with http:// or https://
        asset_info_urls.append((
            ('http://' + asset['info_url'])
            if not asset['info_url'].startswith('http://') and not asset['info_url'].startswith('https://')
            else asset['info_url']))

    asset_info_urls_str = ', '.join(asset_info_urls)
    asset_info_urls_str = (
        (asset_info_urls_str[:2000] + ' ...')
        if len(asset_info_urls_str) > 2000
        else asset_info_urls_str)  # truncate if necessary
    if len(asset_info_urls):
        logger.info('Fetching enhanced asset info for %i assets: %s' % (len(asset_info_urls), asset_info_urls_str))
        util.stream_fetch(
            asset_info_urls, asset_fetch_complete_hook,
            fetch_timeout=10, max_fetch_size=4 * 1024, urls_group_size=20, urls_group_time_spacing=20,
            per_request_complete_callback=lambda url, data: logger.debug("Asset info URL %s retrieved, result: %s" % (url, data)))

    start_task(task_compile_extended_asset_info, delay=60 * 60)  # call again in 60 minutes


@API.add_method
def get_normalized_balances(addresses):
    """
    This call augments counterparty's get_balances with a normalized_quantity field. It also will include any owned
    assets for an address, even if their balance is zero.
    NOTE: Does not retrieve BTC balance. Use get_address_info for that.
    """
    if not isinstance(addresses, list):
        raise Exception("addresses must be a list of addresses, even if it just contains one address")
    if not len(addresses):
        raise Exception("Invalid address list supplied")

    filters = []
    for address in addresses:
        filters.append({'field': 'address', 'op': '==', 'value': address})

    mappings = {}
    result = util.call_jsonrpc_api(
        "get_balances",
        {'filters': filters, 'filterop': 'or'}, abort_on_error=True)['result']

    isowner = {}
    owned_assets = config.mongo_db.tracked_assets.find(
        {'$or': [{'owner': a} for a in addresses]}, {'_history': 0, '_id': 0})
    for o in owned_assets:
        isowner[o['owner'] + o['asset']] = o

    data = []
    for d in result:
        if not d['quantity'] and ((d['address'] + d['asset']) not in isowner):
            continue  # don't include balances with a zero asset value
        asset_info = config.mongo_db.tracked_assets.find_one({'asset': d['asset']})
        divisible = True  # XCP and BTC
        if asset_info and 'divisible' in asset_info:
            divisible = asset_info['divisible']
        d['normalized_quantity'] = blockchain.normalize_quantity(d['quantity'], divisible)
        d['owner'] = (d['address'] + d['asset']) in isowner

        try:
            d['asset_longname'] = asset_info['asset_longname']
        except TypeError as e:
            d['asset_longname'] = d['asset']

        mappings[d['address'] + d['asset']] = d
        data.append(d)

    # include any owned assets for each address, even if their balance is zero
    for key in isowner:
        if key not in mappings:
            o = isowner[key]
            data.append({
                'address': o['owner'],
                'asset': o['asset'],
                'quantity': 0,
                'normalized_quantity': 0,
                'owner': True,
            })

    return data


@API.add_method
def get_escrowed_balances(addresses):
    addresses_holder = ','.join(['?' for e in range(0, len(addresses))])

    sql = '''SELECT (source || '_' || give_asset) AS source_asset, source AS address, give_asset AS asset, SUM(give_remaining) AS quantity
            FROM orders
            WHERE source IN ({}) AND status = ? AND give_asset != ?
            GROUP BY source_asset'''.format(addresses_holder)
    bindings = addresses + ['open', config.BTC]
    results = util.call_jsonrpc_api("sql", {'query': sql, 'bindings': bindings}, abort_on_error=True)['result']

    sql = '''SELECT (tx0_address || '_' || forward_asset) AS source_asset, tx0_address AS address, forward_asset AS asset, SUM(forward_quantity) AS quantity
             FROM order_matches
             WHERE tx0_address IN ({}) AND forward_asset != ? AND status = ?
             GROUP BY source_asset'''.format(addresses_holder)
    bindings = addresses + [config.BTC, 'pending']
    results += util.call_jsonrpc_api("sql", {'query': sql, 'bindings': bindings}, abort_on_error=True)['result']

    sql = '''SELECT (tx1_address || '_' || backward_asset) AS source_asset, tx1_address AS address, backward_asset AS asset, SUM(backward_quantity) AS quantity
             FROM order_matches
             WHERE tx1_address IN ({}) AND backward_asset != ? AND status = ?
             GROUP BY source_asset'''.format(addresses_holder)
    bindings = addresses + [config.BTC, 'pending']
    results += util.call_jsonrpc_api("sql", {'query': sql, 'bindings': bindings}, abort_on_error=True)['result']

    sql = '''SELECT source AS address, '{}' AS asset, SUM(wager_remaining) AS quantity
             FROM bets
             WHERE source IN ({}) AND status = ?
             GROUP BY address'''.format(config.XCP, addresses_holder)
    bindings = addresses + ['open']
    results += util.call_jsonrpc_api("sql", {'query': sql, 'bindings': bindings}, abort_on_error=True)['result']

    sql = '''SELECT tx0_address AS address, '{}' AS asset, SUM(forward_quantity) AS quantity
             FROM bet_matches
             WHERE tx0_address IN ({}) AND status = ?
             GROUP BY address'''.format(config.XCP, addresses_holder)
    bindings = addresses + ['pending']
    results += util.call_jsonrpc_api("sql", {'query': sql, 'bindings': bindings}, abort_on_error=True)['result']

    sql = '''SELECT tx1_address AS address, '{}' AS asset, SUM(backward_quantity) AS quantity
             FROM bet_matches
             WHERE tx1_address IN ({}) AND status = ?
             GROUP BY address'''.format(config.XCP, addresses_holder)
    bindings = addresses + ['pending']
    results += util.call_jsonrpc_api("sql", {'query': sql, 'bindings': bindings}, abort_on_error=True)['result']

    escrowed_balances = {}
    for order in results:
        if order['address'] not in escrowed_balances:
            escrowed_balances[order['address']] = {}
        if order['asset'] not in escrowed_balances[order['address']]:
            escrowed_balances[order['address']][order['asset']] = 0
        escrowed_balances[order['address']][order['asset']] += order['quantity']

    return escrowed_balances


@API.add_method
def get_assets_names_and_longnames():
    ret = []
    assets = config.mongo_db.tracked_assets.find({}, {'_id': 0, '_history': 0})
    for e in assets:
        ret.append({'asset': e['asset'], 'asset_longname': e['asset_longname']})
    return ret


@API.add_method
def get_assets_info(assetsList):
    assets = assetsList  # TODO: change the parameter name at some point in the future...shouldn't be using camel case here
    if not isinstance(assets, list):
        raise Exception("assets must be a list of asset names, even if it just contains one entry")
    assets_info = []
    for asset in assets:
        # BTC and XCP.
        if asset in [config.BTC, config.XCP]:
            if asset == config.BTC:
                supply = blockchain.get_btc_supply(normalize=False)
            else:
                supply = util.call_jsonrpc_api("get_supply", {'asset': config.XCP}, abort_on_error=True)['result']

            assets_info.append({
                'asset': asset,
                'asset_longname': None,
                'owner': None,
                'divisible': True,
                'locked': False,
                'supply': supply,
                'description': '',
                'issuer': None
            })
            continue

        # User-created asset.
        tracked_asset = config.mongo_db.tracked_assets.find_one({'$or': [{'asset': asset}, {'asset_longname': asset}]}, {'_id': 0, '_history': 0})
        if not tracked_asset:
            continue  # asset not found, most likely
        assets_info.append({
            'asset': tracked_asset['asset'],
            'asset_longname': tracked_asset['asset_longname'],
            'owner': tracked_asset['owner'],
            'divisible': tracked_asset['divisible'],
            'locked': tracked_asset['locked'],
            'supply': tracked_asset['total_issued'],
            'description': tracked_asset['description'],
            'issuer': tracked_asset['owner']})
    return assets_info


@API.add_method
def get_base_quote_asset(asset1, asset2):
    """Given two arbitrary assets, returns the base asset and the quote asset.
    """
    # DEPRECATED 1.5
    base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
    base_asset_info = config.mongo_db.tracked_assets.find_one({'asset': base_asset})
    quote_asset_info = config.mongo_db.tracked_assets.find_one({'asset': quote_asset})
    pair_name = "%s/%s" % (base_asset, quote_asset)

    if not base_asset_info or not quote_asset_info:
        raise Exception("Invalid asset(s)")

    return {
        'base_asset': base_asset,
        'quote_asset': quote_asset,
        'pair_name': pair_name
    }


@API.add_method
def get_owned_assets(addresses):
    """Gets a list of owned assets for one or more addresses"""
    result = config.mongo_db.tracked_assets.find({
        'owner': {"$in": addresses}
    }, {"_id": 0}).sort("asset", pymongo.ASCENDING)
    return list(result)


@API.add_method
def get_asset_pair_market_info(asset1=None, asset2=None, limit=50):
    """Given two arbitrary assets, returns the base asset and the quote asset.
    """
    # DEPRECATED 1.5
    assert (asset1 and asset2) or (asset1 is None and asset2 is None)
    if asset1 and asset2:
        base_asset, quote_asset = util.assets_to_asset_pair(asset1, asset2)
        pair_info = config.mongo_db.asset_pair_market_info.find({'base_asset': base_asset, 'quote_asset': quote_asset}, {'_id': 0})
    else:
        pair_info = config.mongo_db.asset_pair_market_info.find({}, {'_id': 0}).sort('completed_trades_count', pymongo.DESCENDING).limit(limit)
        #^ sort by this for now, may want to sort by a market_cap value in the future
    return list(pair_info) or []


@API.add_method
def get_asset_extended_info(asset):
    ext_info = config.mongo_db.asset_extended_info.find_one({'asset': asset}, {'_id': 0})
    return ext_info or False


@API.add_method
def get_asset_history(asset, reverse=False):
    """
    Returns a list of changes for the specified asset, from its inception to the current time.

    @param asset: The asset to retrieve a history on
    @param reverse: By default, the history is returned in the order of oldest to newest. Set this parameter to True
    to return items in the order of newest to oldest.

    @return:
    Changes are returned as a list of dicts, with each dict having the following format:
    * type: One of 'created', 'issued_more', 'changed_description', 'locked', 'transferred', 'called_back'
    * 'at_block': The block number this change took effect
    * 'at_block_time': The block time this change took effect

    * IF type = 'created': Has the following fields, as specified when the asset was initially created:
      * owner, description, divisible, locked, total_issued, total_issued_normalized
    * IF type = 'issued_more':
      * 'additional': The additional quantity issued (raw)
      * 'additional_normalized': The additional quantity issued (normalized)
      * 'total_issued': The total issuance after this change (raw)
      * 'total_issued_normalized': The total issuance after this change (normalized)
    * IF type = 'changed_description':
      * 'prev_description': The old description
      * 'new_description': The new description
    * IF type = 'locked': NO EXTRA FIELDS
    * IF type = 'transferred':
      * 'prev_owner': The address the asset was transferred from
      * 'new_owner': The address the asset was transferred to
    * IF type = 'called_back':
      * 'percentage': The percentage of the asset called back (between 0 and 100)
    """
    asset = config.mongo_db.tracked_assets.find_one({'asset': asset}, {"_id": 0})
    if not asset:
        raise Exception("Unrecognized asset")

    # run down through _history and compose a diff log
    history = []
    raw = asset['_history'] + [asset, ]  # oldest to newest. add on the current state
    prev = None
    for i in range(len(raw)):  # oldest to newest
        if i == 0:
            assert raw[i]['_change_type'] == 'created'
            history.append({
                'type': 'created',
                'owner': raw[i]['owner'],
                'description': raw[i]['description'],
                'divisible': raw[i]['divisible'],
                'locked': raw[i]['locked'],
                'total_issued': raw[i]['total_issued'],
                'total_issued_normalized': raw[i]['total_issued_normalized'],
                'at_block': raw[i]['_at_block'],
                'at_block_time': calendar.timegm(raw[i]['_at_block_time'].timetuple()) * 1000,
            })
            prev = raw[i]
            continue

        assert prev
        if raw[i]['_change_type'] == 'locked':
            history.append({
                'type': 'locked',
                'at_block': raw[i]['_at_block'],
                'at_block_time': calendar.timegm(raw[i]['_at_block_time'].timetuple()) * 1000,
            })
        elif raw[i]['_change_type'] == 'transferred':
            history.append({
                'type': 'transferred',
                'at_block': raw[i]['_at_block'],
                'at_block_time': calendar.timegm(raw[i]['_at_block_time'].timetuple()) * 1000,
                'prev_owner': prev['owner'],
                'new_owner': raw[i]['owner'],
            })
        elif raw[i]['_change_type'] == 'changed_description':
            history.append({
                'type': 'changed_description',
                'at_block': raw[i]['_at_block'],
                'at_block_time': calendar.timegm(raw[i]['_at_block_time'].timetuple()) * 1000,
                'prev_description': prev['description'],
                'new_description': raw[i]['description'],
            })
        else:  # issue additional
            assert raw[i]['total_issued'] - prev['total_issued'] > 0
            history.append({
                'type': 'issued_more',
                'at_block': raw[i]['_at_block'],
                'at_block_time': calendar.timegm(raw[i]['_at_block_time'].timetuple()) * 1000,
                'additional': raw[i]['total_issued'] - prev['total_issued'],
                'additional_normalized': raw[i]['total_issued_normalized'] - prev['total_issued_normalized'],
                'total_issued': raw[i]['total_issued'],
                'total_issued_normalized': raw[i]['total_issued_normalized'],
            })
        prev = raw[i]

    final_history = history
    if reverse:
        final_history.reverse()
    return final_history


@API.add_method
def get_balance_history(asset, addresses, normalize=True, start_ts=None, end_ts=None):
    """Retrieves the ordered balance history for a given address (or list of addresses) and asset pair, within the specified date range
    @param normalize: If set to True, return quantities that (if the asset is divisible) have been divided by 100M (satoshi).
    @return: A list of tuples, with the first entry of each tuple being the block time (epoch TS), and the second being the new balance
     at that block time.
    """
    if not isinstance(addresses, list):
        raise Exception("addresses must be a list of addresses, even if it just contains one address")

    asset_info = config.mongo_db.tracked_assets.find_one({'asset': asset})
    if not asset_info:
        raise Exception("Asset does not exist.")

    now_ts = calendar.timegm(time.gmtime())
    if not end_ts:  # default to current datetime
        end_ts = now_ts
    if not start_ts:  # default to 30 days before the end date
        start_ts = end_ts - (30 * 24 * 60 * 60)
    results = []
    for address in addresses:
        result = config.mongo_db.balance_changes.find({
            'address': address,
            'asset': asset,
            "block_time": {
                "$gte": datetime.datetime.utcfromtimestamp(start_ts)
            } if end_ts == now_ts else {
                "$gte": datetime.datetime.utcfromtimestamp(start_ts),
                "$lte": datetime.datetime.utcfromtimestamp(end_ts)
            }
        }).sort("block_time", pymongo.ASCENDING)
        entry = {
            'name': address,
            'data': [
                (calendar.timegm(r['block_time'].timetuple()) * 1000,
                 r['new_balance_normalized'] if normalize else r['new_balance']
                 ) for r in result]
        }
        results.append(entry)
    return results


@MessageProcessor.subscribe(priority=ASSETS_PRIORITY_PARSE_ISSUANCE)
def parse_issuance(msg, msg_data):
    if msg['category'] != 'issuances':
        return
    if msg_data['status'] != 'valid':
        return

    cur_block_index = config.state['cur_block']['block_index']
    cur_block = config.state['cur_block']

    def modify_extended_asset_info(asset, description):
        """adds an asset to asset_extended_info collection if the description is a valid json link. or, if the link
        is not a valid json link, will remove the asset entry from the table if it exists"""
        if util.is_valid_url(description, suffix='.json', allow_no_protocol=True):
            config.mongo_db.asset_extended_info.update(
                {'asset': asset},
                {'$set': {
                    'info_url': description,
                    'info_status': 'needfetch',
                    'fetch_info_retry': 0,  # retry ASSET_MAX_RETRY times to fetch info from info_url
                    'info_data': {},
                    'errors': []
                }}, upsert=True)
            #^ valid info_status settings: needfetch, valid, invalid, error
            # additional fields will be added later in events, once the asset info is pulled
        else:
            config.mongo_db.asset_extended_info.remove({'asset': asset})
            # remove any saved asset image data
            imagePath = os.path.join(config.data_dir, config.SUBDIR_ASSET_IMAGES, asset + '.png')
            if os.path.exists(imagePath):
                os.remove(imagePath)

    tracked_asset = config.mongo_db.tracked_assets.find_one(
        {'asset': msg_data['asset']}, {'_id': 0, '_history': 0})
    #^ pulls the tracked asset without the _id and history fields. This may be None

    if msg_data['locked'] and (tracked_asset is not None):  # lock asset
        assert tracked_asset is not None
        config.mongo_db.tracked_assets.update(
            {'asset': msg_data['asset']},
            {"$set": {
                '_at_block': cur_block_index,
                '_at_block_time': cur_block['block_time_obj'],
                '_change_type': 'locked',
                'locked': True,
            },
                "$push": {'_history': tracked_asset}}, upsert=False)
        logger.info("Locking asset {}{}".format(msg_data['asset'], ' ({})'.format(msg_data['asset_longname']) if msg_data.get('asset_longname', None) else ''))
    elif msg_data['transfer'] and (tracked_asset is not None):  # transfer asset
        assert tracked_asset is not None
        config.mongo_db.tracked_assets.update(
            {'asset': msg_data['asset']},
            {"$set": {
                '_at_block': cur_block_index,
                '_at_block_time': cur_block['block_time_obj'],
                '_change_type': 'transferred',
                'owner': msg_data['issuer'],
            },
                "$push": {'_history': tracked_asset}}, upsert=False)
        logger.info("Transferring asset {}{} to address {}".format(msg_data['asset'], ' ({})'.format(msg_data['asset_longname']) if msg_data.get('asset_longname', None) else '', msg_data['issuer']))
    elif msg_data['quantity'] == 0 and tracked_asset is not None:  # change description
        config.mongo_db.tracked_assets.update(
            {'asset': msg_data['asset']},
            {"$set": {
                '_at_block': cur_block_index,
                '_at_block_time': cur_block['block_time_obj'],
                '_change_type': 'changed_description',
                'description': msg_data['description'],
            },
                "$push": {'_history': tracked_asset}}, upsert=False)
        modify_extended_asset_info(msg_data['asset'], msg_data['description'])
        logger.info("Changing description for asset {}{} to '{}'".format(msg_data['asset'], ' ({})'.format(msg_data['asset_longname']) if msg_data.get('asset_longname', None) else '', msg_data['description']))
    else:  # issue new asset or issue addition qty of an asset
        if not tracked_asset:  # new issuance
            tracked_asset = {
                '_change_type': 'created',
                '_at_block': cur_block_index,  # the block ID this asset is current for
                '_at_block_time': cur_block['block_time_obj'],
                #^ NOTE: (if there are multiple asset tracked changes updates in a single block for the same
                # asset, the last one with _at_block == that block id in the history array is the
                # final version for that asset at that block
                'asset': msg_data['asset'],
                'asset_longname': msg_data.get('asset_longname', None), # for subassets, this is the full subasset name of the asset, e.g. PIZZA.DOMINOSBLA
                'owner': msg_data['issuer'],
                'description': msg_data['description'],
                'divisible': msg_data['divisible'],
                'locked': msg_data['locked'],
                'total_issued': int(msg_data['quantity']),
                'total_issued_normalized': blockchain.normalize_quantity(msg_data['quantity'], msg_data['divisible']),
                '_history': []  # to allow for block rollbacks
            }
            config.mongo_db.tracked_assets.insert(tracked_asset)
            logger.info("Tracking new asset: {}{}".format(msg_data['asset'], ' ({})'.format(msg_data['asset_longname']) if msg_data.get('asset_longname', None) else ''))
            modify_extended_asset_info(msg_data['asset'], msg_data['description'])
        else:  # issuing additional of existing asset
            assert tracked_asset is not None
            config.mongo_db.tracked_assets.update(
                {'asset': msg_data['asset']},
                {"$set": {
                    '_at_block': cur_block_index,
                    '_at_block_time': cur_block['block_time_obj'],
                    '_change_type': 'issued_more',
                    'divisible': msg_data['divisible'],                 
                },
                    "$inc": {
                    'total_issued': msg_data['quantity'],
                    'total_issued_normalized': blockchain.normalize_quantity(msg_data['quantity'], msg_data['divisible'])
                },
                    "$push": {'_history': tracked_asset}}, upsert=False)
            logger.info("Adding additional {} quantity for asset {}{}".format(blockchain.normalize_quantity(msg_data['quantity'], msg_data['divisible']),
                msg_data['asset'], ' ({})'.format(msg_data['asset_longname']) if msg_data.get('asset_longname', None) else ''))
    return True

@MessageProcessor.subscribe(priority=ASSETS_PRIORITY_PARSE_DESTRUCTION)
def parse_destruction(msg, msg_data):
    if msg['category'] != 'destructions':
        return
    if msg_data['status'] != 'valid':
        return
    if msg_data['asset'] != None and msg_data['asset'] == 'XCP':
        return  

    cur_block_index = config.state['cur_block']['block_index']
    cur_block = config.state['cur_block']

    tracked_asset = config.mongo_db.tracked_assets.find_one(
        {'asset': msg_data['asset']}, {'_id': 0, '_history': 0})
    #^ pulls the tracked asset without the _id and history fields. This may be None

    assert tracked_asset is not None
    config.mongo_db.tracked_assets.update(
        {'asset': msg_data['asset']},
        {"$set": {
            '_at_block': cur_block_index,
            '_at_block_time': cur_block['block_time_obj'],
            '_change_type': 'destruction',
        },
            "$inc": {
            'total_issued': -msg_data['quantity'],
            'total_issued_normalized': blockchain.normalize_quantity(-msg_data['quantity'], tracked_asset['divisible'])
        },
            "$push": {'_history': tracked_asset}}, upsert=False)
    logger.info("Destroying {} quantity of asset {}{}".format(blockchain.normalize_quantity(msg_data['quantity'], tracked_asset['divisible']),
                msg_data['asset'], ' ({})'.format(msg_data['asset_longname']) if msg_data.get('asset_longname', None) else ''))
    return True


@MessageProcessor.subscribe(priority=ASSETS_PRIORITY_BALANCE_CHANGE)  # must come after parse_issuance
def parse_balance_change(msg, msg_data):
    # track balance changes for each address
    bal_change = None
    if msg['category'] in ['credits', 'debits', ]:
        actionName = 'credit' if msg['category'] == 'credits' else 'debit'
        address = msg_data['address']
        asset_info = config.mongo_db.tracked_assets.find_one({'asset': msg_data['asset']})
        if asset_info is None:
            logger.warn("Credit/debit of %s where asset ('%s') does not exist. Ignoring..." % (msg_data['quantity'], msg_data['asset']))
            return 'ABORT_THIS_MESSAGE_PROCESSING'
        quantity = msg_data['quantity'] if msg['category'] == 'credits' else -msg_data['quantity']
        quantity_normalized = blockchain.normalize_quantity(quantity, asset_info['divisible'])

        # look up the previous balance to go off of
        last_bal_change = config.mongo_db.balance_changes.find_one({
            'address': address,
            'asset': asset_info['asset']
        }, sort=[("block_index", pymongo.DESCENDING), ("_id", pymongo.DESCENDING)])

        if last_bal_change \
           and last_bal_change['block_index'] == config.state['cur_block']['block_index']:
            # modify this record, as we want at most one entry per block index for each (address, asset) pair
            last_bal_change['quantity'] += quantity
            last_bal_change['quantity_normalized'] += quantity_normalized
            last_bal_change['new_balance'] += quantity
            last_bal_change['new_balance_normalized'] += quantity_normalized
            config.mongo_db.balance_changes.save(last_bal_change)
            logger.info("%s (UPDATED) %s %s %s %s (new bal: %s, msgID: %s)" % (
                actionName.capitalize(), ('%f' % last_bal_change['quantity_normalized']).rstrip('0').rstrip('.'), last_bal_change['asset'],
                'from' if actionName == 'debit' else 'to',
                last_bal_change['address'], ('%f' % last_bal_change['new_balance_normalized']).rstrip('0').rstrip('.'), msg['message_index'],))
            bal_change = last_bal_change
        else:  # new balance change record for this block
            bal_change = {
                'address': address,
                'asset': asset_info['asset'],
                'asset_longname': asset_info['asset_longname'],
                'block_index': config.state['cur_block']['block_index'],
                'block_time': config.state['cur_block']['block_time_obj'],
                'quantity': quantity,
                'quantity_normalized': quantity_normalized,
                'new_balance': last_bal_change['new_balance'] + quantity if last_bal_change else quantity,
                'new_balance_normalized': last_bal_change['new_balance_normalized'] + quantity_normalized if last_bal_change else quantity_normalized,
            }
            config.mongo_db.balance_changes.insert(bal_change)
            logger.info("%s %s %s %s %s (new bal: %s, msgID: %s)" % (
                actionName.capitalize(), ('%f' % bal_change['quantity_normalized']).rstrip('0').rstrip('.'), bal_change['asset'],
                'from' if actionName == 'debit' else 'to',
                bal_change['address'], ('%f' % bal_change['new_balance_normalized']).rstrip('0').rstrip('.'), msg['message_index'],))


@StartUpProcessor.subscribe()
def init():
    # init db and indexes
    # asset_extended_info
    config.mongo_db.asset_extended_info.ensure_index('asset', unique=True)
    config.mongo_db.asset_extended_info.ensure_index('info_status')
    # balance_changes
    config.mongo_db.balance_changes.ensure_index('block_index')
    config.mongo_db.balance_changes.ensure_index([
        ("address", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
        ("block_index", pymongo.DESCENDING),
        ("_id", pymongo.DESCENDING)
    ])
    #config.mongo_db.balance_changes.ensure_index([
    #    ("address", pymongo.ASCENDING),
    #    ("asset_longname", pymongo.ASCENDING),
    #])
    try:  # drop unnecessary indexes if they exist
        config.mongo_db.balance_changes.drop_index('address_1_asset_1_block_time_1')
    except:
        pass

    # tracked_assets
    config.mongo_db.tracked_assets.ensure_index('asset', unique=True)
    config.mongo_db.tracked_assets.ensure_index('_at_block')  # for tracked asset pruning
    config.mongo_db.tracked_assets.ensure_index([
        ("owner", pymongo.ASCENDING),
        ("asset", pymongo.ASCENDING),
    ])
    # feeds (also init in betting module)
    config.mongo_db.feeds.ensure_index('source')
    config.mongo_db.feeds.ensure_index('owner')
    config.mongo_db.feeds.ensure_index('category')
    config.mongo_db.feeds.ensure_index('info_url')


@CaughtUpProcessor.subscribe()
def start_tasks():
    start_task(task_compile_extended_asset_info)


@RollbackProcessor.subscribe()
def process_rollback(max_block_index):
    if not max_block_index:  # full reparse
        config.mongo_db.balance_changes.drop()
        config.mongo_db.tracked_assets.drop()
        config.mongo_db.asset_extended_info.drop()
        # create XCP and BTC assets in tracked_assets
        for asset in [config.XCP, config.BTC]:
            base_asset = {
                'asset': asset,
                'asset_longname': None,
                'owner': None,
                'divisible': True,
                'locked': False,
                'total_issued': None,
                '_at_block': config.BLOCK_FIRST,  # the block ID this asset is current for
                '_history': []  # to allow for block rollbacks
            }
            config.mongo_db.tracked_assets.insert(base_asset)
    else:  # rollback
        config.mongo_db.balance_changes.remove({"block_index": {"$gt": max_block_index}})

        # to roll back the state of the tracked asset, dive into the history object for each asset that has
        # been updated on or after the block that we are pruning back to
        assets_to_prune = config.mongo_db.tracked_assets.find({'_at_block': {"$gt": max_block_index}})
        for asset in assets_to_prune:
            prev_ver = None
            while len(asset['_history']):
                prev_ver = asset['_history'].pop()
                if prev_ver['_at_block'] <= max_block_index:
                    break
            if not prev_ver or prev_ver['_at_block'] > max_block_index:
                # even the first history version is newer than max_block_index.
                # in this case, just remove the asset tracking record itself
                logger.info("Pruning asset %s (last modified @ block %i, removing as no older state available that is <= block %i)" % (
                    asset['asset'], asset['_at_block'], max_block_index))
                config.mongo_db.tracked_assets.remove({'asset': asset['asset']})
            else:
                # if here, we were able to find a previous version that was saved at or before max_block_index
                # (which should be prev_ver ... restore asset's values to its values
                logger.info("Pruning asset %s (last modified @ block %i, pruning to state at block %i)" % (
                    asset['asset'], asset['_at_block'], max_block_index))
                prev_ver['_id'] = asset['_id']
                prev_ver['_history'] = asset['_history']
                config.mongo_db.tracked_assets.save(prev_ver)
