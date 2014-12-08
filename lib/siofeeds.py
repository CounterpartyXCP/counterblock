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

from lib import config, util

onlineClients = {} #key = walletID, value = datetime when connected
#^ tracks "online status" via the chat feed

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
            
    def disconnect(self, silent=False):
        """Triggered when the client disconnects (e.g. client closes their browser)"""
        self._running = False
        return super(MessagesFeedServerNamespace, self).disconnect(silent=silent)

        
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
    NUM_HISTORY_LINES_NO_REPEAT = 3 #max number of lines to go back ensuring the user is not repeating him/herself
    
    def disconnect(self, silent=False):
        """Triggered when the client disconnects (e.g. client closes their browser)"""
        #record the client as offline
        if 'wallet_id' not in self.socket.session:
            logging.warn("wallet_id not found in socket session: %s" % socket.session)
            return super(ChatFeedServerNamespace, self).disconnect(silent=silent)
        if self.socket.session['wallet_id'] in onlineClients:
            del onlineClients[self.socket.session['wallet_id']]
        return super(ChatFeedServerNamespace, self).disconnect(silent=silent)
    
    def on_ping(self, wallet_id):
        """used to force a triggering of the connection tracking""" 
        #record the client as online
        self.socket.session['wallet_id'] = wallet_id
        onlineClients[wallet_id] = {'when': datetime.datetime.utcnow(), 'state': self}
        return True
    
    def on_start_chatting(self, wallet_id, is_primary_server):
        """this must be the first message sent after connecting to the chat server. Based on the passed
        wallet ID, it will retrieve the chat handle the user initially registered with.
        
        If is_primary_server is specified as True, the user is designating this server as its primary chat server. This means
        that it will be this server that will rebroadcast chat lines to the user (other, non-primary servers will not)
        """
        #normally, wallet ID should be set from on_ping, however if the server goes down and comes back up, this will
        # not be the case for clients already logged in and chatting
        if 'wallet_id' not in self.socket.session: #we specify wallet
            self.socket.session['wallet_id'] = wallet_id
        else:
            assert self.socket.session['wallet_id'] == wallet_id

        #lookup the walletid and ensure that it has a handle match for chat
        chat_profile =  self.request['mongo_db'].chat_handles.find_one({"wallet_id": self.socket.session['wallet_id']})
        handle = chat_profile['handle'] if chat_profile else None
        if not handle:
            return self.error('invalid_id', "No handle is defined for wallet ID %s" % self.socket.session['wallet_id'])
        self.socket.session['is_primary_server'] = is_primary_server
        self.socket.session['handle'] = handle
        self.socket.session['is_op'] = chat_profile.get('is_op', False)
        self.socket.session['banned_until'] = chat_profile.get('banned_until', None)
        self.socket.session['last_action'] = None

    def on_get_lastlines(self):
        return list(self.request['mongo_db'].chat_history.find({}, {'_id': 0}).sort(
            "when", pymongo.DESCENDING).limit(self.NUM_HISTORY_LINES_ON_JOIN))
    
    def on_command(self, command, args):
        """command is the command to run, args is a list of arguments to the command"""
        if 'is_op' not in self.socket.session:
            return self.error('invalid_state', "Invalid state") #this will trigger the client to auto re-establish state
        if command not in ['online', 'msg', 'op', 'unop', 'ban', 'unban', 'handle', 'help', 'disextinfo', 'enextinfo']:
            return self.error('invalid_command', "Unknown command: %s. Try /help for help." % command)
        if command not in ['online', 'msg', 'help'] and not self.socket.session['is_op']:
            return self.error('invalid_access', "Must be an op to use this command")
        
        if command == 'online': #/online <handle>
            if not self.socket.session['is_primary_server']: return
            if len(args) != 1:
                return self.error('invalid_args', "USAGE: /online {handle=} -- Desc: Determines whether a specific user is online")
            handle = args[0]
            p = self.request['mongo_db'].chat_handles.find_one({ 'handle': { '$regex': '^%s$' % handle, '$options': 'i' } })
            if not p:
                return self.error('invalid_args', "Handle '%s' not found" % handle)
            return self.emit("online_status", p['handle'], p['wallet_id'] in onlineClients)
        elif command == 'msg': #/msg <handle> <message text>
            if not self.socket.session['is_primary_server']: return
            if len(args) < 2:
                return self.error('invalid_args', "USAGE: /msg {handle} {private message to send} -- Desc: Sends a private message to a specific user")
            handle = args[0]
            message = ' '.join(args[1:])
            if handle.lower() == self.socket.session['handle'].lower():
                return self.error('invalid_args', "Don't be cray cray and try to message yourself, %s" % handle)
            
            now = datetime.datetime.utcnow()
            if self.socket.session['banned_until'] == -1:
                return self.error('banned', "Your handle is banned from chat indefinitely.")
            if self.socket.session['banned_until'] and self.socket.session['banned_until'] >= now:
                return self.error('banned', "Your handle is still banned from chat for %s more seconds."
                    % int((self.socket.session['banned_until'] - now).total_seconds()))
            
            p = self.request['mongo_db'].chat_handles.find_one({ 'handle': { '$regex': '^%s$' % handle, '$options': 'i' } })
            if not p:
                return self.error('invalid_args', "Handle '%s' not found" % handle)
            if p['wallet_id'] not in onlineClients:
                return self.error('invalid_args', "Handle '%s' is not online" % p['handle'])
            
            message = util.sanitize_eliteness(message[:self.MAX_TEXT_LEN]) #truncate to max allowed and sanitize
            onlineClients[p['wallet_id']]['state'].emit("emote", self.socket.session['handle'],
                message, self.socket.session['is_op'], True, False) #isPrivate = True, viaCommand = False
        elif command in ['op', 'unop']: #/op|unop <handle>
            if len(args) != 1:
                return self.error('invalid_args', "USAGE: /op|unop {handle to op/unop} -- Desc: Gives/removes operator priveledges from a specific user")
            handle = args[0]
            p = self.request['mongo_db'].chat_handles.find_one({ 'handle': { '$regex': '^%s$' % handle, '$options': 'i' } })
            if not p:
                return self.error('invalid_args', "Handle '%s' not found" % handle)
            p['is_op'] = command == 'op'
            self.request['mongo_db'].chat_handles.save(p)
            #make the change active immediately
            handle_lower = handle.lower()
            for sessid, socket in self.socket.server.sockets.iteritems():
                if socket.session.get('handle', None).lower() == handle_lower:
                    socket.session['is_op'] = p['is_op']
            if self.socket.session['is_primary_server']: #let all users know
                self.broadcast_event("oped" if command == "op" else "unoped", self.socket.session['handle'], p['handle'])
        elif command == 'ban': #/ban <handle> <time length in seconds>
            if len(args) != 2:
                return self.error('invalid_args', "USAGE: /ban {handle to ban} {ban_period in sec | -1} -- " +
                    "Desc: Ban a specific user from chatting for a specified period of time, or -1 for unlimited.")
            handle = args[0]
            try:
                ban_period = int(args[1])
                assert ban_period == -1 or (ban_period > 0 and ban_period < 5000000000) 
            except:
                return self.error('invalid_args', "Invalid ban_period value: '%s'" % ban_period)
                
            p = self.request['mongo_db'].chat_handles.find_one({ 'handle': { '$regex': '^%s$' % handle, '$options': 'i' } })
            if not p:
                return self.error('invalid_args', "Handle '%s' not found" % handle)
            p['banned_until'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=ban_period) if ban_period != -1 else -1
            #^ can be the special value of -1 to mean "ban indefinitely"
            self.request['mongo_db'].chat_handles.save(p)
            #make the change active immediately
            handle_lower = handle.lower()
            for sessid, socket in self.socket.server.sockets.iteritems():
                if socket.session.get('handle', None).lower() == handle_lower:
                    socket.session['banned_until'] = p['banned_until']
            if self.socket.session['is_primary_server']: #let all users know
                self.broadcast_event("banned", self.socket.session['handle'], p['handle'], ban_period,
                    int(time.mktime(p['banned_until'].timetuple()))*1000 if p['banned_until'] != -1 else -1);
        elif command == 'unban': #/unban <handle>
            if len(args) != 1:
                return self.error('invalid_args', "USAGE: /unban {handle to unban} -- Desc: Unban a specific banned user")
            handle = args[0]
            p = self.request['mongo_db'].chat_handles.find_one({ 'handle': { '$regex': '^%s$' % handle, '$options': 'i' } })
            if not p:
                return self.error('invalid_args', "Handle '%s' not found" % handle)
            p['banned_until'] = None
            self.request['mongo_db'].chat_handles.save(p)
            #make the change active immediately
            handle_lower = handle.lower()
            for sessid, socket in self.socket.server.sockets.iteritems():
                if socket.session.get('handle', None).lower() == handle_lower:
                    socket.session['banned_until'] = None
            if self.socket.session['is_primary_server']:  #let all users know
                self.broadcast_event("unbanned", self.socket.session['handle'], p['handle'])
        elif command == 'handle': #/handle <oldhandle> <newhandle>
            if len(args) != 2:
                return self.error('invalid_args', "USAGE: /handle {oldhandle} {newhandle} -- Desc: Change a user's handle to something else")
            handle = args[0]
            new_handle = args[1]
            if handle == new_handle:
                return self.error('invalid_args',
                    "The new handle ('%s') must be different than the current handle ('%s')" % (new_handle, handle))
            if not re.match(r'[A-Za-z0-9_-]{4,12}', new_handle):            
                return self.error('invalid_args', "New handle ('%s') contains invalid characters or is not between 4 and 12 characters" % new_handle)
            p = self.request['mongo_db'].chat_handles.find_one({ 'handle': { '$regex': '^%s$' % handle, '$options': 'i' } })
            if not p:
                return self.error('invalid_args', "Handle '%s' not found" % handle)
            new_handle_p = self.request['mongo_db'].chat_handles.find_one({ 'handle': { '$regex': '^%s$' % new_handle, '$options': 'i' } })
            if new_handle_p:
                return self.error('invalid_args', "Handle '%s' already exists" % new_handle)
            old_handle = p['handle'] #has the right capitalization (instead of using 'handle' var)
            p['handle'] = new_handle
            self.request['mongo_db'].chat_handles.save(p)
            #make the change active immediately
            handle_lower = handle.lower()
            for sessid, socket in self.socket.server.sockets.iteritems():
                if socket.session.get('handle', None).lower() == handle_lower:
                    socket.session['handle'] = new_handle
            if self.socket.session['is_primary_server']: #let all users know
                self.broadcast_event("handle_changed", self.socket.session['handle'], old_handle, new_handle)
        elif command in ['enextinfo', 'disextinfo']:
            if len(args) != 1:
                return self.error('invalid_args', "USAGE: /%s {asset} -- Desc: %s" % (command,
                    "Disables extended asset information from showing" if command == 'disextinfo' else "(Re)enables extended asset information"))
            asset = args[0].upper()
            asset_info = self.request['mongo_db'].asset_extended_info.find_one({'asset': asset})
            if not asset_info:
                return self.error('invalid_args', "Asset '%s' has no extended info" % (asset))
            asset_info['disabled'] = command == 'disextinfo'
            self.request['mongo_db'].asset_extended_info.save(asset_info)
            return self.emit("emote", None,
                "Asset '%s' extended info %s" % (asset, 'disabled' if command == 'disextinfo' else 'enabled'),
                False, True) #isPrivate = False, viaCommand = True
        elif command == 'help':
            if self.socket.session['is_op']:
                return self.emit('emote', None,
                    "Valid commands: '/op', '/unop', '/ban', '/unban', '/handle', '/online', '/enextinfo', '/disextinfo'. Type a command alone (i.e. with no arguments) to see the usage.",
                    False, False, True) #isOp = False, isPrivate = False, viaCommand = True
            else:
                return self.emit('emote', None,
                    "Valid commands: '/online', '/msg'. Type a command alone (i.e. with no arguments) to see the usage.",
                    False, False, True) #isOp = False, isPrivate = False, viaCommand = True
        else: assert False #handled earlier
            
    
    def on_emote(self, text):
        if 'handle' not in self.socket.session:
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
            
        #make sure this line hasn't been repeated in the last N chat lines
        past_lines = self.request['mongo_db'].chat_history.find({'handle': self.socket.session['handle']}).sort(
            "when", pymongo.DESCENDING).limit(self.NUM_HISTORY_LINES_NO_REPEAT)
        past_lines = [l['text'] for l in list(past_lines)] if past_lines else []
        
        if text in past_lines: #user is repeating him/herself
            return self.error('repeat_line', "Sorry, this chat message appears to be a duplicate")
        elif (last_message_ago is not None and last_message_ago < self.TIME_BETWEEN_MESSAGES) and not self.socket.session['is_op']: #spamming
            return self.error('too_fast', "Your last message was %i seconds ago (max 1 message every %i seconds)" % (
                last_message_ago, self.TIME_BETWEEN_MESSAGES))
        else: #not spamming, or an op
            text = util.sanitize_eliteness(text[:self.MAX_TEXT_LEN]) #sanitize
            if self.socket.session['is_primary_server']:
                self.broadcast_event('emote', self.socket.session['handle'], text, self.socket.session['is_op'], False, False)
                #^ isPrivate = False, viaCommand = False
            self.socket.session['last_action'] = time.mktime(time.gmtime())
            self.request['mongo_db'].chat_history.insert({
                'handle': self.socket.session['handle'],
                'is_op': self.socket.session['is_op'],
                'text': text,
                'when': self.socket.session['last_action']
            })
        

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
