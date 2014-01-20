#! /usr/bin/env python3
"""
counterwalletd server

Credits for a lot of the zeromq/socket.io plumbing goes to:
   http://sourceforge.net/u/rick446/gevent-socketio/ci/master/tree/demo.py
"""

#import before importing other modules
import gevent
from gevent import monkey; monkey.patch_all()

import os
import argparse
import json
import logging
import appdirs
import configparser
import time
from datetime import datetime

import pymongo
import cube
from socketio import SocketIOServer
from gevent_zeromq import zmq

from lib import (config, api,)


def zmq_server(zmq_context):
    """
    start and run zeromq server, proxying from an inbound TCP connection to either cube or socket.io server
    (via a queue_socketio queue which the socket.io server hangs off of).
    
    any messages sent to queue_socketio, socket.ioapp will get and send off to listening socket.io clients
      (i.e. counterwallet clients so they get a realtime event feed)
    """
    sock_incoming = context.socket(zmq.SUB)
    sock_outgoing_socketio = context.socket(zmq.PUB)
    sock_incoming.bind('tcp://%s:%s' % (config.ZEROMQ_HOST, config.ZEROMQ_PORT))
    sock_outgoing_socketio.bind('inproc://queue_socketio')
    sock_incoming.setsockopt(zmq.SUBSCRIBE, "")
    while True:
        msg = sock_incoming.recv()
        
        #store some data in cube from this???
        
        #determine if socket.io gets the message
        #TODO: massage message data
        #send data to socketio
        sock_outgoing_socketio.send(msg)


class SocketIOApp(object):
    """
    Funnel messages coming from an inproc zmq socket to the socket.io
    """
    def __init__(self, context):
        self.context = context
        
    def __call__(self, environ, start_response):
        if not environ['PATH_INFO'].startswith('/socket.io'):
            start_response('401 UNAUTHORIZED', [])
            return ''
        socketio = environ['socketio']
        sock = self.context.socket(zmq.SUB)
        sock.setsockopt(zmq.SUBSCRIBE, "")
        sock.connect('inproc://queue_socketio')
        while True:
            msg = sock.recv()
            socketio.send(msg)


if __name__ == '__main__':
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(prog='counterwalletd', description='Counterwallet daemon. Works with counterpartyd')
    parser.add_argument('-V', '--version', action='version', version="counterwalletd v%s" % config.VERSION)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='sets log level to DEBUG instead of WARNING')

    parser.add_argument('--data-dir', help='specify to explicitly override the directory in which to keep the config file and log file')
    parser.add_argument('--config-file', help='the location of the configuration file')
    parser.add_argument('--log-file', help='the location of the log file')

    #STUFF WE CONNECT TO
    parser.add_argument('--counterpartyd-rpc-connect', help='the hostname of the counterpartyd JSON-RPC server')
    parser.add_argument('--counterpartyd-rpc-port', type=int, help='the port used to communicate with counterpartyd over JSON-RPC')
    parser.add_argument('--counterpartyd-rpc-user', help='the username used to communicate with counterpartyd over JSON-RPC')
    parser.add_argument('--counterpartyd-rpc-password', help='the password used to communicate with counterpartyd over JSON-RPC')

    parser.add_argument('--mongodb-connect', help='the hostname of the mongodb server to connect to')
    parser.add_argument('--mongodb-port', type=int, help='the port used to communicate with mongodb')
    parser.add_argument('--mongodb-database', help='the mongodb database to connect to')
    parser.add_argument('--mongodb-user', help='the optional username used to communicate with mongodb')
    parser.add_argument('--mongodb-password', help='the optional password used to communicate with mongodb')

    parser.add_argument('--cube-connect', help='the hostname of the Square Cube collector + evaluator')
    parser.add_argument('--cube-collector-port', type=int, help='the port used to communicate with the Square Cube collector')
    parser.add_argument('--cube-evaluator-port', type=int, help='the port used to communicate with the Square Cube evaluator')

    #STUFF WE HOST
    parser.add_argument('--rpc-host', help='the host to provide the counterwalletd JSON-RPC API')
    parser.add_argument('--rpc-port', type=int, help='port on which to provide the counterwalletd JSON-RPC API')
    parser.add_argument('--socketio-host', help='the host to provide the counterwalletd socket.io API')
    parser.add_argument('--socketio-port', type=int, help='port on which to provide the counterwalletd socket.io API')
    parser.add_argument('--zeromq-host', help='the host to provide the counterwalletd zeroMQ broker')
    parser.add_argument('--zeromq-port', type=int, help='port on which to provide the counterwalletd zeroMQ broker')

    args = parser.parse_args()

    # Data directory
    if not args.data_dir:
        config.data_dir = appdirs.user_data_dir(appauthor='Counterparty', appname='counterwalletd', roaming=True)
    else:
        config.data_dir = args.data_dir
    if not os.path.isdir(config.data_dir): os.mkdir(config.data_dir)

    #Read config file
    configfile = configparser.ConfigParser()
    config_path = os.path.join(config.data_dir, 'counterwalletd.conf')
    configfile.read(config_path)
    has_config = 'Default' in configfile

    ##############
    # STUFF WE CONNECT TO

    # counterpartyd RPC host
    if args.counterpartyd_rpc_connect:
        config.COUNTERPARTYD_RPC_CONNECT = args.counterpartyd_rpc_connect
    elif has_config and 'counterpartyd-rpc-connect' in configfile['Default']:
        config.COUNTERPARTYD_RPC_CONNECT = configfile['Default']['counterpartyd-rpc-connect']
    else:
        config.COUNTERPARTYD_RPC_CONNECT = 'localhost'

    # counterpartyd RPC port
    if args.counterpartyd_rpc_port:
        config.COUNTERPARTYD_RPC_PORT = args.counterpartyd_rpc_port
    elif has_config and 'counterpartyd-rpc-port' in configfile['Default']:
        config.COUNTERPARTYD_RPC_PORT = configfile['Default']['counterpartyd-rpc-port']
    else:
        config.COUNTERPARTYD_RPC_PORT = '4000'
    try:
        int(config.COUNTERPARTYD_RPC_PORT)
        assert int(config.COUNTERPARTYD_RPC_PORT) > 1 and int(config.COUNTERPARTYD_RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number counterpartyd-rpc-port configuration parameter")
            
    # counterpartyd RPC user
    if args.counterpartyd_rpc_user:
        config.COUNTERPARTYD_RPC_USER = args.counterpartyd_rpc_user
    elif has_config and 'counterpartyd-rpc-user' in configfile['Default']:
        config.COUNTERPARTYD_RPC_USER = configfile['Default']['counterpartyd-rpc-user']
    else:
        config.COUNTERPARTYD_RPC_USER = 'rpcuser'

    # counterpartyd RPC password
    if args.counterpartyd_rpc_password:
        config.COUNTERPARTYD_RPC_PASSWORD = args.counterpartyd_rpc_password
    elif has_config and 'counterpartyd-rpc-password' in configfile['Default']:
        config.COUNTERPARTYD_RPC_PASSWORD = configfile['Default']['counterpartyd-rpc-password']
    else:
        config.COUNTERPARTYD_RPC_PASSWORD = 'rpcpassword'

    config.COUNTERPARTYD_RPC = 'http://' + config.COUNTERPARTYD_RPC_USER + ':' + config.COUNTERPARTYD_RPC_PASSWORD + '@' + config.COUNTERPARTYD_RPC_CONNECT + ':' + str(config.COUNTERPARTYD_RPC_PORT)

    # mongodb host
    if args.mongodb_connect:
        config.MONGODB_CONNECT = args.mongodb_connect
    elif has_config and 'mongodb-connect' in configfile['Default']:
        config.MONGODB_CONNECT = configfile['Default']['mongodb-connect']
    else:
        config.MONGODB_CONNECT = 'localhost'

    # mongodb port
    if args.mongodb_port:
        config.MONGODB_PORT = args.mongodb_port
    elif has_config and 'mongodb-port' in configfile['Default']:
        config.MONGODB_PORT = configfile['Default']['mongodb-port']
    else:
        config.MONGODB_PORT = '27017'
    try:
        int(config.MONGODB_PORT)
        assert int(config.MONGODB_PORT) > 1 and int(config.MONGODB_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number mongodb-port configuration parameter")
            
    # mongodb database
    if args.mongodb_database:
        config.MONGODB_DATABASE = args.mongodb_database
    elif has_config and 'mongodb-database' in configfile['Default']:
        config.MONGODB_DATABASE = configfile['Default']['mongodb-database']
    else:
        config.MONGODB_DATABASE = 'counterwalletd'

    # mongodb user
    if args.mongodb_user:
        config.MONGODB_USER = args.mongodb_user
    elif has_config and 'mongodb-user' in configfile['Default']:
        config.MONGODB_USER = configfile['Default']['mongodb-user']
    else:
        config.MONGODB_USER = None

    # mongodb password
    if args.mongodb_password:
        config.MONGODB_PASSWORD = args.mongodb_password
    elif has_config and 'mongodb-password' in configfile['Default']:
        config.MONGODB_PASSWORD = configfile['Default']['mongodb-password']
    else:
        config.MONGODB_PASSWORD = None

    # cube host
    if args.cube_connect:
        config.CUBE_CONNECT = args.cube_connect
    elif has_config and 'cube-connect' in configfile['Default']:
        config.CUBE_CONNECT = configfile['Default']['cube-connect']
    else:
        config.CUBE_CONNECT = 'localhost'

    # cube collector port
    if args.cube_collector_port:
        config.CUBE_COLLECTOR_PORT = args.cube_collector_port
    elif has_config and 'cube-collector-port' in configfile['Default']:
        config.CUBE_COLLECTOR_PORT = configfile['Default']['cube-collector-port']
    else:
        config.CUBE_COLLECTOR_PORT = '1080'
    try:
        int(config.CUBE_COLLECTOR_PORT)
        assert int(config.CUBE_COLLECTOR_PORT) > 1 and int(config.CUBE_COLLECTOR_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number cube-collector-port configuration parameter")

    # cube evaluator port
    if args.cube_evaluator_port:
        config.CUBE_EVALUATOR_PORT = args.cube_evaluator_port
    elif has_config and 'cube-evaluator-port' in configfile['Default']:
        config.CUBE_EVALUATOR_PORT = configfile['Default']['cube-evaluator-port']
    else:
        config.CUBE_EVALUATOR_PORT = '1081'
    try:
        int(config.CUBE_EVALUATOR_PORT)
        assert int(config.CUBE_EVALUATOR_PORT) > 1 and int(config.CUBE_EVALUATOR_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number cube-evaluator-port configuration parameter")

    ##############
    # STUFF WE SERVE
    
    # RPC host
    if args.rpc_host:
        config.RPC_HOST = args.rpc_host
    elif has_config and 'rpc-host' in configfile['Default']:
        config.RPC_HOST = configfile['Default']['rpc-host']
    else:
        config.RPC_HOST = 'localhost'

    # RPC port
    if args.rpc_port:
        config.RPC_PORT = args.rpc_port
    elif has_config and 'rpc-port' in configfile['Default']:
        config.RPC_PORT = configfile['Default']['rpc-port']
    else:
        config.RPC_PORT = '4001'
    try:
        int(config.RPC_PORT)
        assert int(config.RPC_PORT) > 1 and int(config.RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number rpc-port configuration parameter")

    # socket.io host
    if args.socketio_host:
        config.SOCKETIO_HOST = args.socketio_host
    elif has_config and 'socketio-host' in configfile['Default']:
        config.SOCKETIO_HOST = configfile['Default']['socketio-host']
    else:
        config.SOCKETIO_HOST = 'localhost'

    # socket.io port
    if args.socketio_port:
        config.SOCKETIO_PORT = args.socketio_port
    elif has_config and 'socketio-port' in configfile['Default']:
        config.SOCKETIO_PORT = configfile['Default']['socketio-port']
    else:
        config.SOCKETIO_PORT = '4002'
    try:
        int(config.SOCKETIO_PORT)
        assert int(config.SOCKETIO_PORT) > 1 and int(config.SOCKETIO_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number socketio-port configuration parameter")

    # zeromq host
    if args.zeromq_host:
        config.ZEROMQ_HOST = args.zeromq_host
    elif has_config and 'zeromq-host' in configfile['Default']:
        config.ZEROMQ_HOST = configfile['Default']['zeromq-host']
    else:
        config.ZEROMQ_HOST = 'localhost'

    # zeromq port
    if args.zeromq_port:
        config.ZEROMQ_PORT = args.zeromq_port
    elif has_config and 'zeromq-port' in configfile['Default']:
        config.ZEROMQ_PORT = configfile['Default']['zeromq-port']
    else:
        config.ZEROMQ_PORT = '4001'
    try:
        int(config.ZEROMQ_PORT)
        assert int(config.ZEROMQ_PORT) > 1 and int(config.ZEROMQ_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number zeromq-port configuration parameter")
  
    # Log
    if args.log_file:
        config.LOG = args.log_file
    elif has_config and 'logfile' in configfile['Default']:
        config.LOG = configfile['Default']['logfile']
    else:
        config.LOG = os.path.join(config.data_dir, 'counterwalletd.log')
    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(filename=config.LOG, level=log_level,
                        format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d-T%H:%M:%S%z')

    # Log also to stderr.
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    requests_log = logging.getLogger("requests")
    requests_log.setLevel(logging.DEBUG if args.verbose else logging.WARNING)

    #Connect to mongodb
    mongo_client = MongoClient(config.MONGODB_HOST, config.MONGODB_PORT)
    try:
        db = mongo_client[config.MONGODB_DATABASE]
    except:
        raise Exception("Specified mongo database (%s) doesn't seem to exist or be accessable" % config.MONGODB_DATABASE)
    if config.MONGODB_USER and config.MONGODB_PASSWORD:
        if not db.authenticate(config.MONGODB_USER, config.MONGODB_PASSWORD):
            raise Exception("Could not authenticate to mongodb with the supplied username and password.")
    #insert mongo indexes if need-be (i.e. for newly created database)
    db.preferences.ensure_index('wallet_id_hash', unique=True)
    
    #Connect to cube
    cube_db = cube.Cube(hostname=config.CUBE_CONNECT,
        collector_port=config.CUBE_COLLECTOR_PORT, evaluator_port=config.CUBE_EVALUATOR_PORT)
    
    if args.action == None: args.action = 'server'

    if args.action == 'help':
        parser.print_help()

    elif args.action == 'server':
        logging.info("Starting up ZeroMQ server...")
        zmq_context = zmq.Context()
        gevent.spawn(zmq_server, zmq_context)
        
        logging.info("Starting up socket.io server...")
        sio_server = SocketIOServer(
            (config.SOCKETIO_HOST, config.SOCKETIO_PORT),
            SocketIOApp(zmq_context), resource="socket.io")        
        sio_server.start() #start the socket.io server greenlets

        logging.info("Starting up RPC API handler...")
        api.serve_api(db)
    else:
        parser.print_help()

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
