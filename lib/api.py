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

REQUIRED_PREFERENCES_FIELDS = ('num_addresses_used', 'address_aliases')

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
    @dispatcher.add_method
    def get_preferences(wallet_id):
        return db.preferences.find_one({"wallet_id": wallet_id}) or {}

    @dispatcher.add_method
    def store_preferences(wallet_id, preferences):
        #preferences should be an object with specific fields
        if    not isinstance(preferences, dict) \
           or not all([f in preferences for f in required_fields]):
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

    
    class Root(object):
        @cherrypy.expose
        @cherrypy.tools.json_out()
        def index(self):
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
    app_config = {
        '/': { 
            'tools.trailing_slash.on': False,
        },
    }
    application = cherrypy.Application(Root(), script_name="/jsonrpc/", config=app_config)
    
    #disable logging of the access and error logs to the screen
    application.log.access_log.propagate = False
    application.log.error_log.propagate = False
    
    #set up a rotating log handler for this application
    # Remove the default FileHandlers if present.
    application.log.error_file = ""
    application.log.access_file = ""
    maxBytes = getattr(application.log, "rot_maxBytes", 10000000)
    backupCount = getattr(application.log, "rot_backupCount", 1000)
    # Make a new RotatingFileHandler for the error log.
    fname = getattr(application.log, "rot_error_file", os.path.join(config.data_dir, "api.error.log"))
    h = logging_handlers.RotatingFileHandler(fname, 'a', maxBytes, backupCount)
    h.setLevel(logging.DEBUG)
    h.setFormatter(cherrypy._cplogging.logfmt)
    application.log.error_log.addHandler(h)
    # Make a new RotatingFileHandler for the access log.
    fname = getattr(application.log, "rot_access_file", os.path.join(config.data_dir, "api.access.log"))
    h = logging_handlers.RotatingFileHandler(fname, 'a', maxBytes, backupCount)
    h.setLevel(logging.DEBUG)
    h.setFormatter(cherrypy._cplogging.logfmt)
    application.log.access_log.addHandler(h)
    
    #start up the API listener/handler
    server = wsgi.WSGIServer(
        (config.RPC_HOST, int(config.RPC_PORT)), application)
    server.serve_forever()
