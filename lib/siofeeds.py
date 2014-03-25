import re
import logging
import datetime
import time
import socket
import collections
import json

import zmq.green as zmq
import pymongo
from socketio import socketio_manage
from socketio.mixins import BroadcastMixin
from socketio.namespace import BaseNamespace


class MessagesFeedServerNamespace(BaseNamespace):
    def __init__(self, *args, **kwargs):
        super(MessagesFeedServerNamespace, self).__init__(*args, **kwargs)
        self._running = True
            
    def listener(self):
        #subscribe to the zmq queue
        sock = self.request['zmq_context'].socket(zmq.SUB)
        sock.setsockopt(zmq.SUBSCRIBE, "")
        sock.connect('inproc://queue_eventfeed')
        
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        
        #as we receive messages, send them out to the socket.io listener
        #do this in a way that doesn't block indefinitely, and gracefully falls through if/when a client disconnects
        while self._running:
            socks = poller.poll(2500) #wait *up to* 2.5 seconds for events to arrive
            if socks:
                event = socks[0][0].recv_json() #only one sock we're polling
                #logging.info("socket.io: Sending message ID %s -- %s:%s" % (
                #    event['_message_index'], event['_category'], event['_command']))
                self.emit(event['_category'], event)
        #sock.shutdown(socket.SHUT_RDWR)
        sock.close()

    def on_subscribe(self):
        if 'listening' not in self.socket.session:
            self.socket.session['listening'] = True
            self.spawn(self.listener)
            
    def recv_disconnect(self):
        """Triggered when we receive a disconnection from the client side (e.g. client closes their browser)"""
        self._running = False            

        
class SocketIOMessagesFeedServer(object):
    """
    Funnel messages coming from counterpartyd polls to socket.io clients
    """
    def __init__(self, zmq_context):
        # Dummy request object to maintain state between Namespace initialization.
        self.request = {
            'zmq_context': zmq_context,
        }        
            
    def __call__(self, environ, start_response):
        if not environ['PATH_INFO'].startswith('/socket.io'):
            start_response('401 UNAUTHORIZED', [])
            return ''
        socketio_manage(environ, {'': MessagesFeedServerNamespace}, self.request)


class ChatFeedServerNamespace(BaseNamespace, BroadcastMixin):
    MAX_TEXT_LEN = 500
    TIME_BETWEEN_MESSAGES = 10 #in seconds (auto-adjust this in the future based on chat speed/volume)
    NUM_HISTORY_LINES_ON_JOIN = 100
    
    def on_set_walletid(self, wallet_id):
        """this must be the first message sent after connecting to the chat server. Based on the passed
        wallet ID, it will retrieve the chat handle the user initially registered with"""
        #set the wallet ID and derive the handle from that
        #lookup the walletid and ensure that it has a handle match for chat
        chat_profile =  self.request['mongo_db'].chat_handles.find_one({"wallet_id": wallet_id})
        handle = chat_profile['handle'] if chat_profile else None
        if not handle:
            return self.error('invalid_id', "No handle is defined for wallet ID %s" % wallet_id)
        self.socket.session['wallet_id'] = wallet_id
        self.socket.session['handle'] = handle
        self.socket.session['op'] = chat_profile.get('op', False)
        self.socket.session['banned_until'] = chat_profile.get('banned_until', None)
        self.socket.session['last_action'] = None

    def on_get_lastlines(self):
        return list(self.request['mongo_db'].chat_history.find({}, {'_id': 0}).sort(
            "when", pymongo.DESCENDING).limit(self.NUM_HISTORY_LINES_ON_JOIN))
    
    def on_command(self, command, args):
        """command is the command to run, args is a list of arguments to the command"""
        if 'op' not in self.socket.session:
            return self.error('invalid_state', "Invalid state") #this will trigger the client to auto re-establish state
        if not self.socket.session['op']:
            return self.error('invalid_access', "Must be an op to use commands")
        if command in ['op', 'unop']: #/op|unop <handle>
            if len(args) != 1:
                return self.error('invalid_args', "USAGE: /op|unop {handle to op/unop}")
            handle = args[0]
            p = self.request['mongo_db'].chat_handles.find_one({"handle": handle})
            if not p:
                return self.error('invalid_args', "Handle '%s' not found" % handle)
            p['op'] = command == 'op'
            self.request['mongo_db'].chat_handles.save(p)
            #make the change active immediately
            for sessid, socket in self.socket.server.sockets.iteritems():
                if socket.session.get('handle', None) == handle:
                    socket.session['op'] = p['op']
            self.broadcast_event("oped" if command == "op" else "unoped", self.socket.session['handle'], handle)
        elif command == 'ban': #/ban <handle> <time length in seconds>
            if len(args) != 2:
                return self.error('invalid_args', "USAGE: /ban {handle to ban} {ban_period in sec | -1}")
            handle = args[0]
            try:
                ban_period = int(args[1])
                assert ban_period == -1 or (ban_period > 0 and ban_period < 5000000000) 
            except:
                return self.error('invalid_args', "Invalid ban_period value: '%s'" % ban_period)
                
            p = self.request['mongo_db'].chat_handles.find_one({"handle": handle})
            if not p:
                return self.error('invalid_args', "Handle '%s' not found" % handle)
            p['banned_until'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=ban_period) if ban_period != -1 else -1
            #^ can be the special value of -1 to mean "ban indefinitely"
            self.request['mongo_db'].chat_handles.save(p)
            #make the change active immediately
            for sessid, socket in self.socket.server.sockets.iteritems():
                if socket.session.get('handle', None) == handle:
                    socket.session['banned_until'] = p['banned_until']
            self.broadcast_event("banned", self.socket.session['handle'], handle, ban_period,
                int(time.mktime(p['banned_until'].timetuple()))*1000 if p['banned_until'] != -1 else -1);
        elif command == 'unban': #/unban <handle>
            if len(args) != 1:
                return self.error('invalid_args', "USAGE: /unban {handle to unban}")
            handle = args[0]
            p = self.request['mongo_db'].chat_handles.find_one({"handle": handle})
            if not p:
                return self.error('invalid_args', "Handle '%s' not found" % handle)
            p['banned_until'] = None
            self.request['mongo_db'].chat_handles.save(p)
            #make the change active immediately
            for sessid, socket in self.socket.server.sockets.iteritems():
                if socket.session.get('handle', None) == handle:
                    socket.session['banned_until'] = None
            self.broadcast_event("unbanned", self.socket.session['handle'], handle)
        elif command == 'handle': #/handle <oldhandle> <newhandle>
            if len(args) != 2:
                return self.error('invalid_args', "USAGE: /handle {oldhandle} {newhandle}")
            handle = args[0]
            new_handle = args[1]
            if handle == new_handle:
                return self.error('invalid_args',
                    "The new handle ('%s') must be different than the current handle ('%s')" % (new_handle, handle))
            if not re.match(r'[A-Za-z0-9_-]{4,12}', new_handle):            
                return self.error('invalid_args', "New handle ('%s') contains invalid characters or is not between 4 and 12 characters" % new_handle)
            p = self.request['mongo_db'].chat_handles.find_one({"handle": handle})
            if not p:
                return self.error('invalid_args', "Handle '%s' not found" % handle)
            new_handle_p = self.request['mongo_db'].chat_handles.find_one({"handle": new_handle})
            if new_handle_p:
                return self.error('invalid_args', "Hanle '%s' already exists" % new_handle)
            p['handle'] = new_handle
            self.request['mongo_db'].chat_handles.save(p)
            #make the change active immediately
            for sessid, socket in self.socket.server.sockets.iteritems():
                if socket.session.get('handle', None) == handle:
                    socket.session['handle'] = new_handle
            self.broadcast_event("handle_changed", self.socket.session['handle'], handle, new_handle)
        elif command == 'help':
            return self.error('invalid_args', "Valid commands: '/op', '/unop', '/ban', '/unban', '/handle'. Type a command alone to see the usage.")
        else:
            return self.error('invalid_command', "Unknown command: %s" % command)
    
    def on_emote(self, text):
        if 'wallet_id' not in self.socket.session:
            return self.error('invalid_state', "Invalid state") #this will trigger the client to auto re-establish state
        
        now = datetime.datetime.utcnow()
        if self.socket.session['banned_until'] == -1:
            return self.error('banned', "Your handle is banned from chat indefinitely.")
        if self.socket.session['banned_until'] and self.socket.session['banned_until'] >= now:
            return self.error('banned', "Your handle is still banned from chat for %s more seconds."
                % int((self.socket.session['banned_until'] - now).total_seconds()))
        
        #make sure this user is not spamming
        if self.socket.session['last_action']:
            last_message_ago = time.mktime(time.gmtime()) - self.socket.session['last_action']
        else:
            last_message_ago = None
        
        if self.socket.session['op'] or (last_message_ago is None or last_message_ago >= self.TIME_BETWEEN_MESSAGES):
            #not spamming, or an op
            #clean up text
            text = text[:self.MAX_TEXT_LEN] #truncate to max allowed
            #TODO: filter out other stuff?
            
            self.broadcast_event_not_me('emote', self.socket.session['handle'], self.socket.session['op'], text)
            self.socket.session['last_action'] = time.mktime(time.gmtime())
            self.request['mongo_db'].chat_history.insert({
                'handle': self.socket.session['handle'],
                'op': self.socket.session['op'],
                'text': text,
                'when': self.socket.session['last_action']
            })
        else: #spamming
            return self.error('too_fast', "Your last message was %i seconds ago (max 1 message every %i seconds)" % (
                last_message_ago, self.TIME_BETWEEN_MESSAGES))
        

class SocketIOChatFeedServer(object):
    """
    Funnel messages from counterparty.io client chats to other clients
    """
    def __init__(self, mongo_db):
        # Dummy request object to maintain state between Namespace initialization.
        self.request = {
            'mongo_db': mongo_db,
        }        
            
    def __call__(self, environ, start_response):
        if not environ['PATH_INFO'].startswith('/socket.io'):
            start_response('401 UNAUTHORIZED', [])
            return ''
        socketio_manage(environ, {'': ChatFeedServerNamespace}, self.request)
