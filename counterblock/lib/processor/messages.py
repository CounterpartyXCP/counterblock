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
        sys.exit(1)  # FOR NOW

    assert msg['message_index'] > config.state['last_message_index']


@MessageProcessor.subscribe(priority=CORE_FIRST_PRIORITY - 1)
def handle_reorg(msg, msg_data):
    if msg['command'] == 'reorg':
        logger.warn("Blockchain reorginization at block %s" % msg_data['block_index'])

        # prune back to and including the specified message_index
        database.rollback(msg_data['block_index'] - 1)
        assert config.state['my_latest_block']['block_index'] == msg_data['block_index'] - 1
        #^ this wil reset config.state['last_message_index'] to -1, which will be restored as we exit the message processing loop

        # abort the current block processing
        return 'ABORT_BLOCK_PROCESSING'
