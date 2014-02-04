import logging
from socketio import socketio_manage
from socketio.mixins import BroadcastMixin
from socketio.namespace import BaseNamespace

class SocketIOEventServer(object):
    """
    Funnel messages coming from counterpartyd polls to socket.io clients
    """
    def __init__(self, to_socketio_queue):
        self.to_socketio_queue = to_socketio_queue
        
    def create_sio_packet(self, msg_type, msg):
        return {
            "type": "event",
            "name": msg_type,
            "args": msg
        }
        
    def __call__(self, environ, start_response):
        if not environ['PATH_INFO'].startswith('/socket.io'):
            start_response('401 UNAUTHORIZED', [])
            return ''
        socketio = environ['socketio']
        while True:
            event = self.to_socketio_queue.get(block=True, timeout=None)
            forwarded_msg = self.create_sio_packet(event['event'], event) #forward over as-is
            logging.debug("socket.io: Sending %s" % forwarded_msg)
            socketio.send_packet(forwarded_msg)


class ChatServerNamespace(BaseNamespace, BroadcastMixin):
    MAX_TEXT_LEN = 500
    TIME_BETWEEN_MESSAGES = 20 #in seconds
    
    def __init__(self, mongo_db, dynamo_chat_handles_table):
        self.mongo_db = mongo_db
        self.dynamo_chat_handles_table = dynamo_chat_handles_table

    def on_set_walletid(self, wallet_id):
        """this must be the first message sent after connecting to the chat server. Based on the passed
        wallet ID, it will retrieve the chat handle the user initially registered with"""
        #set the wallet ID and derive the nickname from that
        #lookup the walletid and ensure that it has a nickname match for chat
        if self.dynamo_chat_handles_table: #dynamodb storage enabled
            try:
                result = self.dynamo_chat_handles_table.get_item(wallet_id=wallet_id)
                handle = result['handle']
            except dynamodb_exceptions.ResourceNotFoundException:
                handle = None
        else: #mongodb-based storage
            result =  self.mongo_db.chat_handles.find_one({"wallet_id": wallet_id})
            handle = result['handle'] if result else None
            
        if not handle:
            return self.error('invalid_id', "No handle is defined for wallet ID %s" % wallet_id)

        self.socket.session['wallet_id'] = wallet_id
        self.socket.session['handle'] = handle
        self.socket.session['last_action'] = None
    
    def on_emote(self, text):
        if 'wallet_id' not in self.socket.session:
            return self.error('invalid_id', "No wallet ID set")
        
        #make sure this user is not spamming
        last_message_ago = (((datetime.datetime.now() - self.socket.session['last_action']).days * 86400)
                 + (datetime.datetime.now() - self.socket.session['last_action']).seconds)
        
        if     self.socket.session['last_action'] \
           and last_message_ago >= TIME_BETWEEN_MESSAGES:
            #clean up text
            text = text[:MAX_TEXT_LEN]
            
            self.broadcast_event_not_me('msg', self.socket.session['handle'], text)
        else: #spamming
            return self.error('too_fast', "Your last message was %i seconds ago (max 1 message every %i seconds)" % (
                last_message_ago, TIME_BETWEEN_MESSAGES))

    #def recv_message(self, message):
    #    print "PING!!!", message
        

class SocketIOChatServer(object):
    """
    Funnel messages from counterparty.io client chats to other clients
    """
    def __init__(self, mongo_db, dynamo_chat_handles_table):
        self.mongo_db = mongo_db
        self.dynamo_chat_handles_table = dynamo_chat_handles_table
            
    def create_sio_packet(self, msg_type, msg):
        return {
            "type": "event",
            "name": msg_type,
            "args": msg
        }
        
    def __call__(self, environ, start_response):
        if not environ['PATH_INFO'].startswith('/socket.io'):
            start_response('401 UNAUTHORIZED', [])
            return ''
        socketio_manage(environ, {'': ChatServerNamespace}, self.mongo_db, self.dynamo_chat_handles_table)
