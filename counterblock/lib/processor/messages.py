import logging
import pymongo
import sys
import gevent
import decimal
D = decimal.Decimal 

from counterblock.lib import util, config, blockchain, blockfeed, database, messages
from counterblock.lib.components import assets, betting
from counterblock.lib.processor import MessageProcessor, CORE_FIRST_PRIORITY, CORE_LAST_PRIORITY, api

logger = logging.getLogger(__name__)

@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 0)
def handle_exceptional(msg, msg_data): 
    if msg['message_index'] != config.state['last_message_index'] + 1 and config.state['last_message_index'] != -1:
        logger.error("BUG: MESSAGE RECEIVED NOT WHAT WE EXPECTED. EXPECTED: %s, GOT: %s: %s (ALL MSGS IN get_messages PAYLOAD: %s)..." % (
            config.state['last_message_index'] + 1, msg['message_index'], msg,
            [m['message_index'] for m in config.state['cur_block']['_messages']]))
        sys.exit(1) #FOR NOW
    
    #BUG: sometimes counterpartyd seems to return OLD messages out of the message feed. deal with those
    if msg['message_index'] <= config.state['last_message_index']:
        logger.warn("BUG: IGNORED old RAW message %s: %s ..." % (msg['message_index'], msg))
        return 'continue'

@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 1)
def handle_invalid(msg, msg_data): 
    #don't process invalid messages, but do forward them along to clients
    status = msg_data.get('status', 'valid').lower()
    if status.startswith('invalid'):
        if config.state['cp_latest_block_index'] - config.state['my_latest_block']['block_index'] < config.MAX_REORG_NUM_BLOCKS:
            #forward along via message feed, except while we're catching up
            event = messages.decorate_message_for_feed(msg, msg_data=msg_data)
            config.ZMQ_PUBLISHER_EVENTFEED.send_json(event)
        config.state['last_message_index'] = msg['message_index']
        return 'continue'
                    
#track message types, for compiling of statistics
@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 2)
def parse_insert(msg, msg_data): 
    if msg['command'] == 'insert' \
       and msg['category'] not in ["debits", "credits", "order_matches", "bet_matches",
           "order_expirations", "bet_expirations", "order_match_expirations", "bet_match_expirations", "bet_match_resolutions"]:
        config.mongo_db.transaction_stats.insert({
            'block_index': config.state['cur_block']['block_index'],
            'block_time': config.state['cur_block']['block_time_obj'],
            'message_index': msg['message_index'],
            'category': msg['category']
        })
    
#HANDLE REORGS
#Works exactly as the original - note the original may not be correctly implemented
@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 3)
def handle_reorg(msg, msg_data):
    if msg['command'] == 'reorg':
        logger.warn("Blockchain reorginization at block %s" % msg_data['block_index'])
        #prune back to and including the specified message_index
        my_latest_block = database.rollback(msg_data['block_index'] - 1)
        config.state['my_latest_block'] = my_latest_block
        assert config.state['my_latest_block']['block_index'] == msg_data['block_index'] - 1

        #for the current last_message_index (which could have gone down after the reorg), query counterpartyd
        running_info = util.jsonrpc_api("get_running_info", abort_on_error=True)['result']
        config.state['last_message_index'] = running_info['last_message_index']
        
        #send out the message to listening clients (but don't forward along while we're catching up)
        if config.state['cp_latest_block_index'] - config.state['my_latest_block']['block_index'] < config.MAX_REORG_NUM_BLOCKS:
            msg_data['_last_message_index'] = config.state['last_message_index']
            event = messages.decorate_message_for_feed(msg, msg_data=msg_data)
            config.ZMQ_PUBLISHER_EVENTFEED.send_json(event)
        return 'break' #break out of inner loop
    
@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 4)
def parse_issuance(msg, msg_data):
    if msg['category'] == 'issuances':
        assets.parse_issuance(config.mongo_db, msg_data, config.state['cur_block']['block_index'], config.state['cur_block'])
    
@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 5)
def parse_balance_change(msg, msg_data): 
    #track balance changes for each address
    bal_change = None
    if msg['category'] in ['credits', 'debits',]:
        actionName = 'credit' if msg['category'] == 'credits' else 'debit'
        address = msg_data['address']
        asset_info = config.mongo_db.tracked_assets.find_one({ 'asset': msg_data['asset'] })
        if asset_info is None:
            logger.warn("Credit/debit of %s where asset ('%s') does not exist. Ignoring..." % (msg_data['quantity'], msg_data['asset']))
            return 'continue'
        quantity = msg_data['quantity'] if msg['category'] == 'credits' else -msg_data['quantity']
        quantity_normalized = blockchain.normalize_quantity(quantity, asset_info['divisible'])

        #look up the previous balance to go off of
        last_bal_change = config.mongo_db.balance_changes.find_one({
            'address': address,
            'asset': asset_info['asset']
        }, sort=[("block_index", pymongo.DESCENDING), ("_id", pymongo.DESCENDING)])
        
        if last_bal_change \
           and last_bal_change['block_index'] == config.state['cur_block']['block_index']:
            #modify this record, as we want at most one entry per block index for each (address, asset) pair
            last_bal_change['quantity'] += quantity
            last_bal_change['quantity_normalized'] += quantity_normalized
            last_bal_change['new_balance'] += quantity
            last_bal_change['new_balance_normalized'] += quantity_normalized
            config.mongo_db.balance_changes.save(last_bal_change)
            logger.info("Procesed %s bal change (UPDATED) from tx %s :: %s" % (actionName, msg['message_index'], last_bal_change))
            bal_change = last_bal_change
        else: #new balance change record for this block
            bal_change = {
                'address': address, 
                'asset': asset_info['asset'],
                'block_index': config.state['cur_block']['block_index'],
                'block_time': config.state['cur_block']['block_time_obj'],
                'quantity': quantity,
                'quantity_normalized': quantity_normalized,
                'new_balance': last_bal_change['new_balance'] + quantity if last_bal_change else quantity,
                'new_balance_normalized': last_bal_change['new_balance_normalized'] + quantity_normalized if last_bal_change else quantity_normalized,
            }
            config.mongo_db.balance_changes.insert(bal_change)
            logger.info("Procesed %s bal change from tx %s :: %s" % (actionName, msg['message_index'], bal_change))
    
@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 6)
def parse_trade_book(msg, msg_data):
    #book trades
    if (msg['category'] == 'order_matches'
        and ((msg['command'] == 'update' and msg_data['status'] == 'completed') #for a trade with BTC involved, but that is settled (completed)
             or ('forward_asset' in msg_data and msg_data['forward_asset'] != config.BTC and msg_data['backward_asset'] != config.BTC))): #or for a trade without BTC on either end

        if msg['command'] == 'update' and msg_data['status'] == 'completed':
            #an order is being updated to a completed status (i.e. a BTCpay has completed)
            tx0_hash, tx1_hash = msg_data['order_match_id'][:64], msg_data['order_match_id'][65:]
            #get the order_match this btcpay settles
            order_match = util.jsonrpc_api("get_order_matches",
                {'filters': [
                 {'field': 'tx0_hash', 'op': '==', 'value': tx0_hash},
                 {'field': 'tx1_hash', 'op': '==', 'value': tx1_hash}]
                }, abort_on_error=False)['result'][0]
        else:
            assert msg_data['status'] == 'completed' #should not enter a pending state for non BTC matches
            order_match = msg_data

        forward_asset_info = config.mongo_db.tracked_assets.find_one({'asset': order_match['forward_asset']})
        backward_asset_info = config.mongo_db.tracked_assets.find_one({'asset': order_match['backward_asset']})
        assert forward_asset_info and backward_asset_info
        base_asset, quote_asset = util.assets_to_asset_pair(order_match['forward_asset'], order_match['backward_asset'])
        
        #don't create trade records from order matches with BTC that are under the dust limit
        if    (order_match['forward_asset'] == config.BTC and order_match['forward_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF) \
           or (order_match['backward_asset'] == config.BTC and order_match['backward_quantity'] <= config.ORDER_BTC_DUST_LIMIT_CUTOFF):
            logger.debug("Order match %s ignored due to %s under dust limit." % (order_match['tx0_hash'] + order_match['tx1_hash'], config.BTC))
            return 'continue'

        #take divisible trade quantities to floating point
        forward_quantity = blockchain.normalize_quantity(order_match['forward_quantity'], forward_asset_info['divisible'])
        backward_quantity = blockchain.normalize_quantity(order_match['backward_quantity'], backward_asset_info['divisible'])
        
        #compose trade
        trade = {
            'block_index': config.state['cur_block']['block_index'],
            'block_time': config.state['cur_block']['block_time_obj'],
            'message_index': msg['message_index'], #secondary temporaral ordering off of when
            'order_match_id': order_match['tx0_hash'] + '_' + order_match['tx1_hash'],
            'order_match_tx0_index': order_match['tx0_index'],
            'order_match_tx1_index': order_match['tx1_index'],
            'order_match_tx0_address': order_match['tx0_address'],
            'order_match_tx1_address': order_match['tx1_address'],
            'base_asset': base_asset,
            'quote_asset': quote_asset,
            'base_quantity': order_match['forward_quantity'] if order_match['forward_asset'] == base_asset else order_match['backward_quantity'],
            'quote_quantity': order_match['backward_quantity'] if order_match['forward_asset'] == base_asset else order_match['forward_quantity'],
            'base_quantity_normalized': forward_quantity if order_match['forward_asset'] == base_asset else backward_quantity,
            'quote_quantity_normalized': backward_quantity if order_match['forward_asset'] == base_asset else forward_quantity,
        }
        trade['unit_price'] = float(
            ( D(trade['quote_quantity_normalized']) / D(trade['base_quantity_normalized']) ).quantize(
                D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))
        trade['unit_price_inverse'] = float(
            ( D(trade['base_quantity_normalized']) / D(trade['quote_quantity_normalized']) ).quantize(
                D('.00000000'), rounding=decimal.ROUND_HALF_EVEN))

        config.mongo_db.trades.insert(trade)
        logger.info("Procesed Trade from tx %s :: %s" % (msg['message_index'], trade))
        
@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 7)
def parse_broadcast(msg,msg_data): 
    if msg['category'] == 'broadcasts':
        betting.parse_broadcast(config.mongo_db, msg_data)

@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 8)
def parse_for_socketio(msg, msg_data):
    #if we're catching up beyond MAX_REORG_NUM_BLOCKS blocks out, make sure not to send out any socket.io
    # events, as to not flood on a resync (as we may give a 525 to kick the logged in clients out, but we
    # can't guarantee that the socket.io connection will always be severed as well??)
    if config.state['cp_latest_block_index'] - config.state['my_latest_block']['block_index'] < config.MAX_REORG_NUM_BLOCKS:
        #send out the message to listening clients
        event = messages.decorate_message_for_feed(msg, msg_data=msg_data)
        config.ZMQ_PUBLISHER_EVENTFEED.send_json(event)

