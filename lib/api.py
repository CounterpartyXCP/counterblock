#import before importing other modules
import os
import json
import logging
from logging import handlers as logging_handlers
import requests
import cherrypy
from jsonrpc import JSONRPCResponseManager, dispatcher
from gevent import wsgi

from . import (config,)

def call_jsonrpc_api(method, params, endpoint=None, auth=None):
    if not endpoint: endpoint = config.COUNTERPARTYD_RPC
    if not auth: auth = config.COUNTERPARTYD_AUTH
    payload = {
      "method": method,
      "params": params,
        "jsonrpc": "2.0",
        "id": 0,
    }
    response = requests.post(
        endpoint, data=json.dumps(payload), headers={'content-type': 'application/json'}, auth=auth).json()
    return response

def serve_api(db):
    # Preferneces are just JSON objects... since we don't force a specific form to the wallet on
    # the server side, this makes it easier for 3rd party wallets (i.e. not counterwallet) to fully be able to
    # use counterwalletd to not only pull useful data, but also load and store their own preferences, containing
    # whatever data they need
    
    @dispatcher.add_method
    def get_preferences(wallet_id):
        result =  db.preferences.find_one({"wallet_id": wallet_id}) or {}
        return result

    @dispatcher.add_method
    def store_preferences(wallet_id, preferences):
        print "preferences", preferences
        if not isinstance(preferences, dict):
            raise Exception("Invalid preferences object")
        try:
            preferences_json = json.dumps(preferences)
        except:
            raise Exception("Cannot dump preferences to JSON")
        
        #store/update this in the db
        db.preferences.update({'wallet_id': wallet_id},
            {"$set": {'wallet_id': wallet_id, 'preferences': preferences_json}},
            upsert=True)
        return True
    
    @dispatcher.add_method
    def get_asset_owner(asset):
        #gets the current owner for the given asset
        response = call_jsonrpc_api(method,
            {
                'filters': {'field': 'asset', 'op': '==', 'value': asset},
                'order_by': 'block_index',
                'order_dir': 'desc'
            }
        )
        
        #get the last issurance message for this asset, which should reflect the current owner
        if 'result' in response and len(response['result']):
            return response['result'][0]['issuer']
        else: #error, or asset doesn't exist
            return None
    
    @dispatcher.add_method
    def proxy_to_counterpartyd(method, params):
        return call_jsonrpc_api(method, params)
    
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
            logging.debug("JSON RPC Server Response: ", response.json)
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
        {'/': { 'tools.trailing_slash.on': False, 'request.dispatch': cherrypy.dispatch.Dispatcher()}})    
    cherrypy.tree.mount(apiApplication, '/jsonrpc/',
        {'/': { 'tools.trailing_slash.on': False, 'request.dispatch': cherrypy.dispatch.Dispatcher()}})    
    
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
