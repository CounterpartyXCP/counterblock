import logging
import pymongo
import sys
import gevent
import decimal

from counterblock.lib import util, config, blockchain, blockfeed, database, messages
from counterblock.lib.processor import MessageProcessor, CORE_FIRST_PRIORITY, CORE_LAST_PRIORITY, api

D = decimal.Decimal 
logger = logging.getLogger(__name__)

@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 0)
def do_sanity_checks(msg, msg_data): 
    if msg['message_index'] != config.state['last_message_index'] + 1 and config.state['last_message_index'] != -1:
        logger.error("BUG: MESSAGE RECEIVED NOT WHAT WE EXPECTED. EXPECTED: %s, GOT: %s: %s (ALL MSGS IN get_messages PAYLOAD: %s)..." % (
            config.state['last_message_index'] + 1, msg['message_index'], msg,
            [m['message_index'] for m in config.state['cur_block']['_messages']]))
        sys.exit(1) #FOR NOW
    
    assert msg['message_index'] > config.state['last_message_index']

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
                        
@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 2)
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
