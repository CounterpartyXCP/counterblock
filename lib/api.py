#import before importing other modules
import os
import json
import base64
import logging
from logging import handlers as logging_handlers
import requests
from gevent import wsgi
import cherrypy
from cherrypy.process import plugins
from jsonrpc import JSONRPCResponseManager, dispatcher
from bson import json_util
from boto.dynamodb import exceptions as dynamodb_exceptions

from . import (config,)

def call_jsonrpc_api(method, params, endpoint=None, auth=None):
    if not endpoint: endpoint = config.COUNTERPARTYD_RPC
    if not auth: auth = config.COUNTERPARTYD_AUTH
    payload = {
      "id": 0,
      "jsonrpc": "2.0",
      "method": method,
      "params": params,
    }
    r = requests.post(
        endpoint, data=json.dumps(payload), headers={'content-type': 'application/json'}, auth=auth)
    if r.status_code != 200:
        raise Exception("Bad status code returned from counterwalletd: %s. payload: %s" % (r.status_code, r.text))
    return r.json()

def serve_api(mongo_db, dynamo_preferences_table, redis_client):
    # Preferneces are just JSON objects... since we don't force a specific form to the wallet on
    # the server side, this makes it easier for 3rd party wallets (i.e. not counterwallet) to fully be able to
    # use counterwalletd to not only pull useful data, but also load and store their own preferences, containing
    # whatever data they need
    
    DEFAULT_COUNTERPARTYD_API_CACHE_PERIOD = 60 #in seconds

    @dispatcher.add_method
    def get_preferences(wallet_id):
        if dynamo_preferences_table: #dynamodb storage enabled
            try:
                result = dynamo_preferences_table.get_item(hash_key=wallet_id)
                return json.loads(result['preferences'])
            except dynamodb_exceptions.DynamoDBKeyNotFoundError:
                return {}
        else: #mongodb-based storage
            result =  mongo_db.preferences.find_one({"wallet_id": wallet_id})
            return json.loads(result['preferences']) if result else {}

    @dispatcher.add_method
    def store_preferences(wallet_id, preferences):
        if not isinstance(preferences, dict):
            raise Exception("Invalid preferences object")
        try:
            preferences_json = json.dumps(preferences)
        except:
            raise Exception("Cannot dump preferences to JSON")
        
        if dynamo_preferences_table: #dynamodb storage enabled
            try:
                prefs = dynamo_preferences_table.get_item(hash_key=wallet_id)
                prefs['preferences'] = preferences_json
                prefs.put() #update
            except dynamodb_exceptions.DynamoDBKeyNotFoundError: #insert new
                prefs = dynamo_preferences_table.new_item(hash_key=wallet_id,
                    attrs={'preferences': preferences_json})
                prefs.put()
        else: #mongodb-based storage
            mongo_db.preferences.update(
                {'wallet_id': wallet_id},
                {"$set": {'wallet_id': wallet_id, 'preferences': preferences_json}}, upsert=True)
        return True
    
    @dispatcher.add_method
    def proxy_to_counterpartyd(method='', params=[]):
        raw_result = None
        cache_key = None

        if redis_client: #check for a precached result and send that back instead
            cache_key = method + '||' + base64.b64encode(json.dumps(params).encode()).decode()
            #^ must use encoding (e.g. base64) since redis doesn't allow spaces in its key names
            # (also shortens the hashing key for better performance)
            raw_result = redis_client.get(cache_key)
        
        if raw_result is None: #cache miss or cache disabled
            raw_result = call_jsonrpc_api(method, params)
            if redis_client: #cache miss
                redis_client.setex(cache_key, DEFAULT_COUNTERPARTYD_API_CACHE_PERIOD, raw_result)
                #^TODO: we may want to have different cache periods for different types of data
        
        result = json.loads(raw_result)
        if 'error' in result:
            raise Exception(result['error']['data'].get('message', result['error']['message']))
        return result['result']
    
    class Root(object):
        @cherrypy.expose
        def index(self):
            return ''

    class API(object):
        @cherrypy.expose
        @cherrypy.tools.json_out()
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
            return response.json
                
    
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
