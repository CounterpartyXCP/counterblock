import logging
import datetime
import time
import collections
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
        if not environ['PATH_INFO'].startswith('/_feed'):
            start_response('401 UNAUTHORIZED', [])
            return ''
        socketio = environ['socketio']
        while True:
            event = self.to_socketio_queue.get(block=True, timeout=None)
            event['msg']['_message_index'] = event['message_index']
            event['msg']['_block_index'] = event['block_index']
            event['msg']['_block_time'] = event['block_time']
            event['msg']['_command'] = event['command']
            forwarded_msg = self.create_sio_packet(event['event'], event['msg']) #forward over as-is
            logging.info("socket.io: Sending message ID %s -- %s" % (
                event['msg']['_message_index'], event['msg']['_command']))
            socketio.send_packet(forwarded_msg)


class ChatServerNamespace(BaseNamespace, BroadcastMixin):
    MAX_TEXT_LEN = 500
    TIME_BETWEEN_MESSAGES = 10 #in seconds (auto-adjust this in the future based on chat speed/volume)
    
    def on_set_walletid(self, wallet_id):
        """this must be the first message sent after connecting to the chat server. Based on the passed
        wallet ID, it will retrieve the chat handle the user initially registered with"""
        #set the wallet ID and derive the nickname from that
        #lookup the walletid and ensure that it has a nickname match for chat
        result =  self.request['mongo_db'].chat_handles.find_one({"wallet_id": wallet_id})
        handle = result['handle'] if result else None
        if not handle:
            return self.error('invalid_id', "No handle is defined for wallet ID %s" % wallet_id)
        self.socket.session['wallet_id'] = wallet_id
        self.socket.session['handle'] = handle
        self.socket.session['last_action'] = None

    def on_get_lastlines(self):
        return list(self.request['last_chats'])
    
    def on_emote(self, text):
        if 'wallet_id' not in self.socket.session:
            return self.error('invalid_id', "No wallet ID set")
        
        #make sure this user is not spamming
        if self.socket.session['last_action']:
            last_message_ago = time.mktime(time.gmtime()) - self.socket.session['last_action']
        else:
            last_message_ago = None
        
        if last_message_ago is None or last_message_ago >= self.TIME_BETWEEN_MESSAGES: #not spamming
            #clean up text
            text = text[:self.MAX_TEXT_LEN] #truncate to max allowed
            #TODO: filter out other stuff?
            
            self.broadcast_event_not_me('emote', self.socket.session['handle'], text)
            self.socket.session['last_action'] = time.mktime(time.gmtime())
            self.request['last_chats'].append({'handle': self.socket.session['handle'],
                'text': text, 'when': self.socket.session['last_action']})
        else: #spamming
            return self.error('too_fast', "Your last message was %i seconds ago (max 1 message every %i seconds)" % (
                last_message_ago, self.TIME_BETWEEN_MESSAGES))
        

class SocketIOChatServer(object):
    """
    Funnel messages from counterparty.io client chats to other clients
    """
    def __init__(self, mongo_db):
        # Dummy request object to maintain state between Namespace initialization.
        self.request = {
            'mongo_db': mongo_db,
            'last_chats': collections.deque(maxlen=100), 
        }        
            
    def __call__(self, environ, start_response):
        if not environ['PATH_INFO'].startswith('/_chat'):
            start_response('401 UNAUTHORIZED', [])
            return ''
        socketio_manage(environ, {'': ChatServerNamespace}, self.request)
