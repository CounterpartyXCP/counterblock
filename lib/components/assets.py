import os
import logging
import decimal
import base64
import json
from datetime import datetime

from lib import config, util, util_bitcoin

ASSET_MAX_RETRY = 3
D = decimal.Decimal

def parse_issuance(db, message, cur_block_index, cur_block):
    if message['status'] != 'valid':
        return

    def modify_extended_asset_info(asset, description):
        """adds an asset to asset_extended_info collection if the description is a valid json link. or, if the link
        is not a valid json link, will remove the asset entry from the table if it exists"""
        if util.is_valid_url(description, suffix='.json', allow_no_protocol=True):
            db.asset_extended_info.update({'asset': asset},
                {'$set': {
                    'info_url': description,
                    'info_status': 'needfetch',
                    'fetch_info_retry': 0, # retry ASSET_MAX_RETRY times to fetch info from info_url
                    'info_data': {},
                    'errors': []
                }}, upsert=True)
            #^ valid info_status settings: needfetch, valid, invalid, error
            #additional fields will be added later in events, once the asset info is pulled
        else:
            db.asset_extended_info.remove({ 'asset': asset })
            #remove any saved asset image data
            imagePath = os.path.join(config.DATA_DIR, config.SUBDIR_ASSET_IMAGES, asset + '.png')
            if os.path.exists(imagePath):
                os.remove(imagePath)

    tracked_asset = db.tracked_assets.find_one(
        {'asset': message['asset']}, {'_id': 0, '_history': 0})
    #^ pulls the tracked asset without the _id and history fields. This may be None
    
    if message['locked']: #lock asset
        assert tracked_asset is not None
        db.tracked_assets.update(
            {'asset': message['asset']},
            {"$set": {
                '_at_block': cur_block_index,
                '_at_block_time': cur_block['block_time_obj'], 
                '_change_type': 'locked',
                'locked': True,
             },
             "$push": {'_history': tracked_asset } }, upsert=False)
        logging.info("Locking asset %s" % (message['asset'],))
    elif message['transfer']: #transfer asset
        assert tracked_asset is not None
        db.tracked_assets.update(
            {'asset': message['asset']},
            {"$set": {
                '_at_block': cur_block_index,
                '_at_block_time': cur_block['block_time_obj'], 
                '_change_type': 'transferred',
                'owner': message['issuer'],
             },
             "$push": {'_history': tracked_asset } }, upsert=False)
        logging.info("Transferring asset %s to address %s" % (message['asset'], message['issuer']))
    elif message['quantity'] == 0 and tracked_asset is not None: #change description
        db.tracked_assets.update(
            {'asset': message['asset']},
            {"$set": {
                '_at_block': cur_block_index,
                '_at_block_time': cur_block['block_time_obj'], 
                '_change_type': 'changed_description',
                'description': message['description'],
             },
             "$push": {'_history': tracked_asset } }, upsert=False)
        modify_extended_asset_info(message['asset'], message['description'])
        logging.info("Changing description for asset %s to '%s'" % (message['asset'], message['description']))
    else: #issue new asset or issue addition qty of an asset
        if not tracked_asset: #new issuance
            tracked_asset = {
                '_change_type': 'created',
                '_at_block': cur_block_index, #the block ID this asset is current for
                '_at_block_time': cur_block['block_time_obj'], 
                #^ NOTE: (if there are multiple asset tracked changes updates in a single block for the same
                # asset, the last one with _at_block == that block id in the history array is the
                # final version for that asset at that block
                'asset': message['asset'],
                'owner': message['issuer'],
                'description': message['description'],
                'divisible': message['divisible'],
                'locked': False,
                'total_issued': message['quantity'],
                'total_issued_normalized': util_bitcoin.normalize_quantity(message['quantity'], message['divisible']),
                '_history': [] #to allow for block rollbacks
            }
            db.tracked_assets.insert(tracked_asset)
            logging.info("Tracking new asset: %s" % message['asset'])
            modify_extended_asset_info(message['asset'], message['description'])
        else: #issuing additional of existing asset
            assert tracked_asset is not None
            db.tracked_assets.update(
                {'asset': message['asset']},
                {"$set": {
                    '_at_block': cur_block_index,
                    '_at_block_time': cur_block['block_time_obj'], 
                    '_change_type': 'issued_more',
                 },
                 "$inc": {
                     'total_issued': message['quantity'],
                     'total_issued_normalized': util_bitcoin.normalize_quantity(message['quantity'], message['divisible'])
                 },
                 "$push": {'_history': tracked_asset} }, upsert=False)
            logging.info("Adding additional %s quantity for asset %s" % (
                util_bitcoin.normalize_quantity(message['quantity'], message['divisible']), message['asset']))
    return True

def inc_fetch_retry(db, asset, max_retry=ASSET_MAX_RETRY, new_status='error', errors=[]):
    asset['fetch_info_retry'] += 1
    asset['errors'] = errors
    if asset['fetch_info_retry'] == max_retry:
        asset['info_status'] = new_status
    db.asset_extended_info.save(asset)

def sanitize_json_data(data):
    data['asset'] = util.sanitize_eliteness(data['asset'])
    if 'description' in data: data['description'] = util.sanitize_eliteness(data['description'])
    if 'website' in data: data['website'] = util.sanitize_eliteness(data['website'])
    if 'pgpsig' in data: data['pgpsig'] = util.sanitize_eliteness(data['pgpsig'])
    return data

def process_asset_info(db, asset, info_data):
    # sanity check
    assert asset['info_status'] == 'needfetch'
    assert 'info_url' in asset
    assert util.is_valid_url(asset['info_url'], allow_no_protocol=True) #already validated in the fetch

    errors = util.is_valid_json(info_data, config.ASSET_SCHEMA)
    
    if not isinstance(info_data, dict) or 'asset' not in info_data:
        errors.append('Invalid data format')
    elif asset['asset'] != info_data['asset']:
        errors.append('asset field does not match asset name')
   
    if len(errors) > 0:
        inc_fetch_retry(db, asset, new_status='invalid', errors=errors)
        return (False, errors) 

    asset['info_status'] = 'valid'

    #fetch any associated images...
    #TODO: parallelize this 2nd level asset image fetching ... (e.g. just compose a list here, and process it in later on)
    if 'image' in info_data:
        info_data['valid_image'] = util.fetch_image(info_data['image'],
            config.SUBDIR_ASSET_IMAGES, asset['asset'], fetch_timeout=5)
        
    asset['info_data'] = sanitize_json_data(info_data)
    db.asset_extended_info.save(asset)
    return (True, None)

def fetch_all_asset_info(db):
    assets = list(db.asset_extended_info.find({'info_status': 'needfetch'}))
    asset_info_urls = []

    def asset_fetch_complete_hook(urls_data):
        logging.info("Enhanced asset info fetching complete. %s unique URLs fetched. Processing..." % len(urls_data))
        for asset in assets:
            logging.debug("Looking at asset %s: %s" % (asset, asset['info_url']))
            if asset['info_url']:
                info_url = ('http://' + asset['info_url']) \
                    if not asset['info_url'].startswith('http://') and not asset['info_url'].startswith('https://') else asset['info_url']
                assert info_url in urls_data
                if not urls_data[info_url][0]: #request was not successful
                    inc_fetch_retry(db, asset, max_retry=ASSET_MAX_RETRY, errors=[urls_data[info_url][1]])
                    logging.warn("Fetch for asset at %s not successful: %s (try %i of %i)" % (
                        info_url, urls_data[info_url][1], asset['fetch_info_retry'], ASSET_MAX_RETRY))
                else:
                    result = process_asset_info(db, asset, urls_data[info_url][1])
                    if not result[0]:
                        logging.info("Processing for asset %s at %s not successful: %s" % (asset['asset'], info_url, result[1]))
                    else:
                        logging.info("Processing for asset %s at %s successful" % (asset['asset'], info_url))
        
    #compose and fetch all info URLs in all assets with them
    for asset in assets:
        if not asset['info_url']: continue
        
        if asset.get('disabled', False):
            logging.info("ExtendedAssetInfo: Skipping disabled asset %s" % asset['asset'])
            continue

        #may or may not end with .json. may or may not start with http:// or https://
        asset_info_urls.append(('http://' + asset['info_url']) \
            if not asset['info_url'].startswith('http://') and not asset['info_url'].startswith('https://') else asset['info_url'])

    asset_info_urls_str = ', '.join(asset_info_urls)
    asset_info_urls_str = (asset_info_urls_str[:2000] + ' ...') if len(asset_info_urls_str) > 2000 else asset_info_urls_str #truncate if necessary
    if len(asset_info_urls):
        logging.info('Fetching enhanced asset info for %i assets: %s' % (len(asset_info_urls), asset_info_urls_str))
        util.stream_fetch(asset_info_urls, asset_fetch_complete_hook,
            fetch_timeout=10, max_fetch_size=4*1024, urls_group_size=20, urls_group_time_spacing=20,
            per_request_complete_callback=lambda url, data: logging.debug("Asset info URL %s retrieved, result: %s" % (url, data)))


def get_escrowed_balances(addresses):

    addresses_holder = ','.join(['?' for e in range(0,len(addresses))])

    sql ='''SELECT (source || '_' || give_asset) AS source_asset, source AS address, give_asset AS asset, SUM(give_remaining) AS quantity
            FROM orders
            WHERE source IN ({}) AND status = ? AND give_asset != ?
            GROUP BY source_asset'''.format(addresses_holder)
    bindings = addresses + ['open', 'BTC']
    results = util.call_jsonrpc_api("sql", {'query': sql, 'bindings': bindings}, abort_on_error=True)['result']

    sql = '''SELECT (tx0_address || '_' || forward_asset) AS source_asset, tx0_address AS address, forward_asset AS asset, SUM(forward_quantity) AS quantity
             FROM order_matches
             WHERE tx0_address IN ({}) AND forward_asset != ? AND status = ?
             GROUP BY source_asset'''.format(addresses_holder)
    bindings = addresses + ['BTC', 'pending']
    results += util.call_jsonrpc_api("sql", {'query': sql, 'bindings': bindings}, abort_on_error=True)['result']

    sql = '''SELECT (tx1_address || '_' || backward_asset) AS source_asset, tx1_address AS address, backward_asset AS asset, SUM(backward_quantity) AS quantity
             FROM order_matches
             WHERE tx1_address IN ({}) AND backward_asset != ? AND status = ?
             GROUP BY source_asset'''.format(addresses_holder)
    bindings = addresses + ['BTC', 'pending']
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

    sql = '''SELECT source AS address, '{}' AS asset, SUM(wager) AS quantity
             FROM rps
             WHERE source IN ({}) AND status = ?
             GROUP BY address'''.format(config.XCP, addresses_holder)
    bindings = addresses + ['open']
    results += util.call_jsonrpc_api("sql", {'query': sql, 'bindings': bindings}, abort_on_error=True)['result']

    sql = '''SELECT tx0_address AS address, '{}' AS asset, SUM(wager) AS quantity
             FROM rps_matches
             WHERE tx0_address IN ({}) AND status IN (?, ?, ?)
             GROUP BY address'''.format(config.XCP, addresses_holder)
    bindings = addresses + ['pending', 'pending and resolved', 'resolved and pending']
    results += util.call_jsonrpc_api("sql", {'query': sql, 'bindings': bindings}, abort_on_error=True)['result']

    sql = '''SELECT tx1_address AS address, '{}' AS asset, SUM(wager) AS quantity
             FROM rps_matches
             WHERE tx1_address IN ({}) AND status IN (?, ?, ?)
             GROUP BY address'''.format(config.XCP, addresses_holder)
    bindings = addresses + ['pending', 'pending and resolved', 'resolved and pending']
    results += util.call_jsonrpc_api("sql", {'query': sql, 'bindings': bindings}, abort_on_error=True)['result']

    escrowed_balances = {}
    for order in results:
        if order['address'] not in escrowed_balances:
            escrowed_balances[order['address']] = {}
        if order['asset'] not in escrowed_balances[order['address']]:
            escrowed_balances[order['address']][order['asset']] = 0
        escrowed_balances[order['address']][order['asset']] += order['quantity']

    return escrowed_balances
