import os
import json
import re
import time
import datetime
import base64
import logging
from logging import handlers as logging_handlers
import requests
from gevent import wsgi
import cherrypy
from cherrypy.process import plugins
from jsonrpc import JSONRPCResponseManager, dispatcher
from bson import json_util

from . import (config, util)

PREFERENCES_MAX_LENGTH = 100000 #in bytes, as expressed in JSON

def serve_api(mongo_db, redis_client):
    # Preferneces are just JSON objects... since we don't force a specific form to the wallet on
    # the server side, this makes it easier for 3rd party wallets (i.e. not counterwallet) to fully be able to
    # use counterwalletd to not only pull useful data, but also load and store their own preferences, containing
    # whatever data they need
    
    DEFAULT_COUNTERPARTYD_API_CACHE_PERIOD = 60 #in seconds
    
    @dispatcher.add_method
    def get_chat_handle(wallet_id):
        result = mongo_db.chat_handles.find_one({"wallet_id": wallet_id})
        return {
            'handle': result['handle'],
            'last_updated': result.get('last_updated', None)
            } if result else {}

    @dispatcher.add_method
    def store_chat_handle(wallet_id, handle):
        """Set or update a chat handle"""
        if not isinstance(handle, basestring):
            raise Exception("Invalid chat handle: bad data type")
        if not re.match(r'[A-Za-z0-9_-]{4,12}', handle):
            raise Exception("Invalid chat handle: bad syntax/length")

        mongo_db.chat_handles.update(
            {'wallet_id': wallet_id},
            {"$set": {
                'wallet_id': wallet_id,
                'handle': handle,
                'last_updated': time.mktime(time.gmtime()) 
                }
            }, upsert=True)
        #^ last_updated MUST be in GMT, as it will be compaired again other servers
        return True

    @dispatcher.add_method
    def get_preferences(wallet_id):
        result =  mongo_db.preferences.find_one({"wallet_id": wallet_id})
        return {
            'preferences': json.loads(result['preferences']),
            'last_updated': result.get('last_updated', None)
            } if result else {'preferences': {}, 'last_updated': None}

    @dispatcher.add_method
    def store_preferences(wallet_id, preferences):
        if not isinstance(preferences, dict):
            raise Exception("Invalid preferences object")
        try:
            preferences_json = json.dumps(preferences)
        except:
            raise Exception("Cannot dump preferences to JSON")
        
        #sanity check around max size
        if len(preferences_json) >= PREFERENCES_MAX_LENGTH:
            raise Exception("Preferences object is too big.")
        
        mongo_db.preferences.update(
            {'wallet_id': wallet_id},
            {"$set": {
                'wallet_id': wallet_id,
                'preferences': preferences_json,
                'last_updated': time.mktime(time.gmtime())
                }
            }, upsert=True)
        #^ last_updated MUST be in GMT, as it will be compaired again other servers
        return True
    
    @dispatcher.add_method
    def proxy_to_counterpartyd(method='', params=[]):
        result = None
        cache_key = None

        if redis_client: #check for a precached result and send that back instead
            cache_key = method + '||' + base64.b64encode(json.dumps(params).encode()).decode()
            #^ must use encoding (e.g. base64) since redis doesn't allow spaces in its key names
            # (also shortens the hashing key for better performance)
            result = redis_client.get(cache_key)
            if result:
                try:
                    result = json.loads(result)
                except Exception, e:
                    logging.warn("Error loading JSON from cache: %s, cached data: '%s'" % (e, result))
                    result = None #skip from reading from cache and just make the API call
        
        if result is None: #cache miss or cache disabled
            result = util.call_jsonrpc_api(method, params)
            if redis_client: #cache miss
                redis_client.setex(cache_key, DEFAULT_COUNTERPARTYD_API_CACHE_PERIOD, json.dumps(result))
                #^TODO: we may want to have different cache periods for different types of data
        
        if 'error' in result:
            raise Exception(result['error']['data'].get('message', result['error']['message']))
        return result['result']
    
    class Root(object):
        @cherrypy.expose
        def index(self):
            return ''

    class API(object):
        @cherrypy.expose
        def index(self):
            cherrypy.response.headers["Content-Type"] = 'application/json' 
            cherrypy.response.headers["Access-Control-Allow-Origin"] = '*' 
            cherrypy.response.headers["Access-Control-Allow-Methods"] = 'POST, GET, OPTIONS'
            cherrypy.response.headers["Access-Control-Allow-Headers"] = 'Origin, X-Requested-With, Content-Type, Accept'

            if cherrypy.request.method == "OPTIONS": #web client will send us this before making a request
                return

            try:
                data = cherrypy.request.body.read().decode('utf-8')
            except ValueError:
                raise cherrypy.HTTPError(400, 'Invalid JSON document')
            response = JSONRPCResponseManager.handle(data, dispatcher)
            return response.json.encode()
                
    
    cherrypy.config.update({
        'log.screen': False,
        "environment": "embedded",
        'log.error_log.propagate': False,
        'log.access_log.propagate': False,
        "server.logToScreen" : False
    })
    rootApplication = cherrypy.Application(Root(), script_name="/")
    apiApplication = cherrypy.Application(API(), script_name="/jsonrpc/")
    cherrypy.tree.mount(rootApplication, '/',
        {'/': { 'tools.trailing_slash.on': False,
                'request.dispatch': cherrypy.dispatch.Dispatcher()}})    
    cherrypy.tree.mount(apiApplication, '/jsonrpc/',
        {'/': { 'tools.trailing_slash.on': False,
                'request.dispatch': cherrypy.dispatch.Dispatcher()}})
    
    #disable logging of the access and error logs to the screen
    rootApplication.log.access_log.propagate = False
    rootApplication.log.error_log.propagate = False
    apiApplication.log.access_log.propagate = False
    apiApplication.log.error_log.propagate = False
    
    #set up a rotating log handler for this application
    # Remove the default FileHandlers if present.
    apiApplication.log.error_file = ""
    apiApplication.log.access_file = ""
    maxBytes = getattr(apiApplication.log, "rot_maxBytes", 10000000)
    backupCount = getattr(apiApplication.log, "rot_backupCount", 1000)
    # Make a new RotatingFileHandler for the error log.
    fname = getattr(apiApplication.log, "rot_error_file", os.path.join(config.data_dir, "api.error.log"))
    h = logging_handlers.RotatingFileHandler(fname, 'a', maxBytes, backupCount)
    h.setLevel(logging.DEBUG)
    h.setFormatter(cherrypy._cplogging.logfmt)
    apiApplication.log.error_log.addHandler(h)
    # Make a new RotatingFileHandler for the access log.
    fname = getattr(apiApplication.log, "rot_access_file", os.path.join(config.data_dir, "api.access.log"))
    h = logging_handlers.RotatingFileHandler(fname, 'a', maxBytes, backupCount)
    h.setLevel(logging.DEBUG)
    h.setFormatter(cherrypy._cplogging.logfmt)
    apiApplication.log.access_log.addHandler(h)
    
    #start up the API listener/handler
    server = wsgi.WSGIServer(
        (config.RPC_HOST, int(config.RPC_PORT)), cherrypy.tree)
    server.serve_forever()
