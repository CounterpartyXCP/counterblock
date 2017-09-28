import json
import copy
import logging
import pymongo

from counterblock.lib import config, blockchain, database

logger = logging.getLogger(__name__)


def decorate_message(message, for_txn_history=False):
    # insert custom fields in certain events...
    # even invalid actions need these extra fields for proper reporting to the client (as the reporting message
    # is produced via PendingActionViewModel.calcText) -- however make it able to deal with the queried data not existing in this case
    assert '_category' in message
    if for_txn_history:
        message['_command'] = 'insert'  # history data doesn't include this
        block_index = message['block_index'] if 'block_index' in message else message['tx1_block_index']
        message['_block_time'] = database.get_block_time(block_index)
        message['_tx_index'] = message['tx_index'] if 'tx_index' in message else message.get('tx1_index', None)
        if message['_category'] in ['bet_expirations', 'order_expirations', 'bet_match_expirations', 'order_match_expirations']:
            message['_tx_index'] = 0  # add tx_index to all entries (so we can sort on it secondarily in history view), since these lack it

    # include asset extended information (longname and divisible)
    for attr in ('asset', 'get_asset', 'give_asset', 'forward_asset', 'backward_asset', 'dividend_asset'):
        if attr not in message:
            continue
        asset_info = config.mongo_db.tracked_assets.find_one({'asset': message[attr]})
        message['_{}_longname'.format(attr)] = asset_info['asset_longname'] if asset_info else None
        message['_{}_divisible'.format(attr)] = asset_info['divisible'] if asset_info else None

    if message['_category'] in ['credits', 'debits']:
        # find the last balance change on record
        bal_change = config.mongo_db.balance_changes.find_one(
            {'address': message['address'], 'asset': message['asset']},
            sort=[("block_time", pymongo.DESCENDING)])
        message['_quantity_normalized'] = abs(bal_change['quantity_normalized']) if bal_change else None
        message['_balance'] = bal_change['new_balance'] if bal_change else None
        message['_balance_normalized'] = bal_change['new_balance_normalized'] if bal_change else None

    if message['_category'] in ['orders', 'order_matches', ]:
        message['_btc_below_dust_limit'] = (
            ('forward_asset' in message and message['forward_asset'] == config.BTC and message['forward_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF)
            or ('backward_asset' in message and message['backward_asset'] == config.BTC and message['backward_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF)
        )

    if message['_category'] in ['issuances', ]:
        message['_quantity_normalized'] = blockchain.normalize_quantity(message['quantity'], message['divisible'])
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
    message['_block_time'] = database.get_block_time(msg['block_index'])
    message['_category'] = msg['category']
    message['_status'] = msg_data.get('status', 'valid')
    message = decorate_message(message)
    return message


def get_address_cols_for_entity(entity):
    if entity in ['debits', 'credits']:
        return ['address', ]
    elif entity in ['issuances', ]:
        return ['issuer', ]
    elif entity in ['sends', 'dividends', 'bets', 'cancels', 'orders', 'burns', 'broadcasts', 'btcpays']:
        return ['source', ]
    # elif entity in ['order_matches', 'bet_matches']:
    elif entity in ['order_matches', 'order_expirations', 'order_match_expirations',
                    'bet_matches', 'bet_expirations', 'bet_match_expirations']:
        return ['tx0_address', 'tx1_address']
    else:
        raise Exception("Unknown entity type: %s" % entity)
