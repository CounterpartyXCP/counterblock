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
import ConfigParser
import time
from datetime import datetime

import pymongo
import cube
from requests.auth import HTTPBasicAuth
from socketio import server as socketio_server
import zmq.green as zmq

from lib import (config, api,)


def zmq_subscriber(zmq_context):
    """
    start and run zeromq subscriber, connecting to the counterpartyd zeromq publisher and proxying from this
    to either cube or socket.io server (via a queue_socketio queue which the socket.io server hangs off of).
    
    any messages sent to queue_socketio, socket.ioapp will get and send off to listening socket.io clients
      (i.e. counterwallet clients so they get a realtime event feed)
    """
    #listen on our socketio queue
    publisher_socketio = zmq_context.socket(zmq.PUB)
    publisher_socketio.bind('inproc://queue_socketio')
    
    def recv_or_timeout(subscriber, timeout_ms):
        poller = zmq.Poller()
        poller.register(subscriber, zmq.POLLIN)
        while True:
            socket = dict(poller.poll(timeout_ms))
            if socket.get(subscriber) == zmq.POLLIN:
                msg = subscriber.recv_json()
                logging.info("Event feed received message: %s" % msg['_TYPE'])
                
                #store some data in cube from this???
                #TODO!!
                
                #send the message to the socket.io processor for it to process/massage and forward on to
                # web clients as necessary
                publisher_socketio.send_json(msg)
            else:
                return # Timeout!
            
    while True:
        #connect to counterpartyd's zeromq publisher
        subscriber = zmq_context.socket(zmq.SUB)
        url = 'tcp://%s:%s' % (config.ZEROMQ_CONNECT, config.ZEROMQ_PORT)
        logging.info("Connecting to counterpartyd realtime (ZeroMQ) event feed @ %s" % url)
        subscriber.connect(url)
        logging.info("Connected to counterpartyd realtime (ZeroMQ) event feed")
        subscriber.setsockopt(zmq.SUBSCRIBE, "") #clear filter
        
        recv_or_timeout(subscriber, None) #this will block until timeout of some sort (timeout disabled currently)
        subscriber.close()        
        logging.warning("counterpartyd realtime event feed connection broken/timeout. Reconnecting...")


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
            msg = sock.recv_json()
            
            #TODO: logic here to process and massage the data before sending on to clients
            if msg['_TYPE'] in ['debit', 'credit']:
                #forward over as-is
                forwarded_msg = msg
            else:
                #ignore for now
                forwarded_msg = None
            
            if forwarded_msg:
                socketio.send(forwarded_msg, json=True)


if __name__ == '__main__':
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(prog='counterwalletd', description='Counterwallet daemon. Works with counterpartyd')
    parser.add_argument('-V', '--version', action='version', version="counterwalletd v%s" % config.VERSION)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='sets log level to DEBUG instead of WARNING')

    parser.add_argument('--data-dir', help='specify to explicitly override the directory in which to keep the config file and log file')
    parser.add_argument('--config-file', help='the location of the configuration file')
    parser.add_argument('--log-file', help='the location of the log file')

    #STUFF WE CONNECT TO
    parser.add_argument('--bitcoind-rpc-connect', help='the hostname of the Bitcoind JSON-RPC server')
    parser.add_argument('--bitcoind-rpc-port', type=int, help='the port used to communicate with Bitcoind over JSON-RPC')
    parser.add_argument('--bitcoind-rpc-user', help='the username used to communicate with Bitcoind over JSON-RPC')
    parser.add_argument('--bitcoind-rpc-password', help='the password used to communicate with Bitcoind over JSON-RPC')

    parser.add_argument('--counterpartyd-rpc-connect', help='the hostname of the counterpartyd JSON-RPC server')
    parser.add_argument('--counterpartyd-rpc-port', type=int, help='the port used to communicate with counterpartyd over JSON-RPC')
    parser.add_argument('--counterpartyd-rpc-user', help='the username used to communicate with counterpartyd over JSON-RPC')
    parser.add_argument('--counterpartyd-rpc-password', help='the password used to communicate with counterpartyd over JSON-RPC')

    parser.add_argument('--mongodb-connect', help='the hostname of the mongodb server to connect to')
    parser.add_argument('--mongodb-port', type=int, help='the port used to communicate with mongodb')
    parser.add_argument('--mongodb-database', help='the mongodb database to connect to')
    parser.add_argument('--mongodb-user', help='the optional username used to communicate with mongodb')
    parser.add_argument('--mongodb-password', help='the optional password used to communicate with mongodb')

    parser.add_argument('--zeromq-connect', help='the hostname of the counterpartyd server hosting zeromq')
    parser.add_argument('--zeromq-port', type=int, help='the port used to connect to the counterpartyd server hosting zeromq')

    parser.add_argument('--cube-connect', help='the hostname of the Square Cube collector + evaluator')
    parser.add_argument('--cube-collector-port', type=int, help='the port used to communicate with the Square Cube collector')
    parser.add_argument('--cube-evaluator-port', type=int, help='the port used to communicate with the Square Cube evaluator')

    #STUFF WE HOST
    parser.add_argument('--rpc-host', help='the host to provide the counterwalletd JSON-RPC API')
    parser.add_argument('--rpc-port', type=int, help='port on which to provide the counterwalletd JSON-RPC API')
    parser.add_argument('--socketio-host', help='the host to provide the counterwalletd socket.io API')
    parser.add_argument('--socketio-port', type=int, help='port on which to provide the counterwalletd socket.io API')

    args = parser.parse_args()

    # Data directory
    if not args.data_dir:
        config.data_dir = appdirs.user_data_dir(appauthor='Counterparty', appname='counterwalletd', roaming=True)
    else:
        config.data_dir = args.data_dir
    if not os.path.isdir(config.data_dir): os.mkdir(config.data_dir)

    #Read config file
    configfile = ConfigParser.ConfigParser()
    config_path = os.path.join(config.data_dir, 'counterwalletd.conf')
    configfile.read(config_path)
    has_config = configfile.has_section('Default')

    ##############
    # STUFF WE CONNECT TO

    # Bitcoind RPC host
    if args.bitcoind_rpc_connect:
        config.BITCOIND_RPC_CONNECT = args.bitcoind_rpc_connect
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-connect'):
        config.BITCOIND_RPC_CONNECT = configfile.get('Default', 'bitcoind-rpc-connect')
    else:
        config.BITCOIND_RPC_CONNECT = 'localhost'

    # Bitcoind RPC port
    if args.bitcoind_rpc_port:
        config.BITCOIND_RPC_PORT = args.bitcoind_rpc_port
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-port') and configfile.get('Default', 'bitcoind-rpc-port'):
        config.BITCOIND_RPC_PORT = configfile.get('Default', 'bitcoind-rpc-port')
    else:
        config.BITCOIND_RPC_PORT = '8332'
    try:
        int(config.BITCOIND_RPC_PORT)
        assert int(config.BITCOIND_RPC_PORT) > 1 and int(config.BITCOIND_RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number bitcoind-rpc-port configuration parameter")
            
    # Bitcoind RPC user
    if args.bitcoind_rpc_user:
        config.BITCOIND_RPC_USER = args.bitcoind_rpc_user
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-user'):
        config.BITCOIND_RPC_USER = configfile.get('Default', 'bitcoind-rpc-user')
    else:
        config.BITCOIND_RPC_USER = 'bitcoinrpc'

    # Bitcoind RPC password
    if args.bitcoind_rpc_password:
        config.BITCOIND_RPC_PASSWORD = args.bitcoind_rpc_password
    elif has_config and configfile.has_option('Default', 'bitcoind-rpc-password'):
        config.BITCOIND_RPC_PASSWORD = configfile.get('Default', 'bitcoind-rpc-password')
    else:
        raise Exception('bitcoind RPC password not set. (Use configuration file or --bitcoind-rpc-password=PASSWORD)')

    config.BITCOIND_RPC = 'http://' + config.BITCOIND_RPC_CONNECT + ':' + str(config.BITCOIND_RPC_PORT)
    config.BITCOIND_AUTH = HTTPBasicAuth(config.BITCOIND_RPC_USER, config.BITCOIND_RPC_PASSWORD) if (config.BITCOIND_RPC_USER and config.BITCOIND_RPC_PASSWORD) else None

    # counterpartyd RPC host
    if args.counterpartyd_rpc_connect:
        config.COUNTERPARTYD_RPC_CONNECT = args.counterpartyd_rpc_connect
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-connect'):
        config.COUNTERPARTYD_RPC_CONNECT = configfile.get('Default', 'counterpartyd-rpc-connect')
    else:
        config.COUNTERPARTYD_RPC_CONNECT = 'localhost'

    # counterpartyd RPC port
    if args.counterpartyd_rpc_port:
        config.COUNTERPARTYD_RPC_PORT = args.counterpartyd_rpc_port
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-port') and configfile.get('Default', 'counterpartyd-rpc-port'):
        config.COUNTERPARTYD_RPC_PORT = configfile.get('Default', 'counterpartyd-rpc-port')
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
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-user'):
        config.COUNTERPARTYD_RPC_USER = configfile.get('Default', 'counterpartyd-rpc-user')
    else:
        config.COUNTERPARTYD_RPC_USER = 'rpcuser'

    # counterpartyd RPC password
    if args.counterpartyd_rpc_password:
        config.COUNTERPARTYD_RPC_PASSWORD = args.counterpartyd_rpc_password
    elif has_config and configfile.has_option('Default', 'counterpartyd-rpc-password'):
        config.COUNTERPARTYD_RPC_PASSWORD = configfile.get('Default', 'counterpartyd-rpc-password')
    else:
        config.COUNTERPARTYD_RPC_PASSWORD = 'rpcpassword'

    config.COUNTERPARTYD_RPC = 'http://' + config.COUNTERPARTYD_RPC_CONNECT + ':' + str(config.COUNTERPARTYD_RPC_PORT)
    config.COUNTERPARTYD_AUTH = HTTPBasicAuth(config.COUNTERPARTYD_RPC_USER, config.COUNTERPARTYD_RPC_PASSWORD) if (config.COUNTERPARTYD_RPC_USER and config.COUNTERPARTYD_RPC_PASSWORD) else None

    # mongodb host
    if args.mongodb_connect:
        config.MONGODB_CONNECT = args.mongodb_connect
    elif has_config and configfile.has_option('Default', 'mongodb-connect'):
        config.MONGODB_CONNECT = configfile.get('Default', 'mongodb-connect')
    else:
        config.MONGODB_CONNECT = 'localhost'

    # mongodb port
    if args.mongodb_port:
        config.MONGODB_PORT = args.mongodb_port
    elif has_config and configfile.has_option('Default', 'mongodb-port') and configfile.get('Default', 'mongodb-port'):
        config.MONGODB_PORT = configfile.get('Default', 'mongodb-port')
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
    elif has_config and configfile.has_option('Default', 'mongodb-database'):
        config.MONGODB_DATABASE = configfile.get('Default', 'mongodb-database')
    else:
        config.MONGODB_DATABASE = 'counterwalletd'

    # mongodb user
    if args.mongodb_user:
        config.MONGODB_USER = args.mongodb_user
    elif has_config and configfile.has_option('Default', 'mongodb-user'):
        config.MONGODB_USER = configfile.get('Default', 'mongodb-user')
    else:
        config.MONGODB_USER = None

    # mongodb password
    if args.mongodb_password:
        config.MONGODB_PASSWORD = args.mongodb_password
    elif has_config and configfile.has_option('Default', 'mongodb-password'):
        config.MONGODB_PASSWORD = configfile.get('Default', 'mongodb-password')
    else:
        config.MONGODB_PASSWORD = None

    # zeromq host
    if args.zeromq_connect:
        config.ZEROMQ_CONNECT = args.zeromq_connect
    elif has_config and configfile.has_option('Default', 'zeromq-connect'):
        config.ZEROMQ_CONNECT = configfile.get('Default', 'zeromq-connect')
    else:
        config.ZEROMQ_CONNECT = '127.0.0.1'

    # zeromq port
    if args.zeromq_port:
        config.ZEROMQ_PORT = args.zeromq_port
    elif has_config and configfile.has_option('Default', 'zeromq-port') and configfile.get('Default', 'zeromq-port'):
        config.ZEROMQ_PORT = configfile.get('Default', 'zeromq-port')
    else:
        config.ZEROMQ_PORT = '4001'
    try:
        int(config.ZEROMQ_PORT)
        assert int(config.ZEROMQ_PORT) > 1 and int(config.ZEROMQ_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number zeromq-port configuration parameter")

    # cube host
    if args.cube_connect:
        config.CUBE_CONNECT = args.cube_connect
    elif has_config and configfile.has_option('Default', 'cube-connect'):
        config.CUBE_CONNECT = configfile.get('Default', 'cube-connect')
    else:
        config.CUBE_CONNECT = 'localhost'

    # cube collector port
    if args.cube_collector_port:
        config.CUBE_COLLECTOR_PORT = args.cube_collector_port
    elif has_config and configfile.has_option('Default', 'cube-collector-port') and configfile.get('Default', 'cube-collector-port'):
        config.CUBE_COLLECTOR_PORT = configfile.get('Default', 'cube-collector-port')
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
    elif has_config and configfile.has_option('Default', 'cube-evaluator-port') and configfile.get('Default', 'cube-evaluator-port'):
        config.CUBE_EVALUATOR_PORT = configfile.get('Default', 'cube-evaluator-port')
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
    elif has_config and configfile.has_option('Default', 'rpc-host'):
        config.RPC_HOST = configfile.get('Default', 'rpc-host')
    else:
        config.RPC_HOST = 'localhost'

    # RPC port
    if args.rpc_port:
        config.RPC_PORT = args.rpc_port
    elif has_config and configfile.has_option('Default', 'rpc-port') and configfile.get('Default', 'rpc-port'):
        config.RPC_PORT = configfile.get('Default', 'rpc-port')
    else:
        config.RPC_PORT = '4100'
    try:
        int(config.RPC_PORT)
        assert int(config.RPC_PORT) > 1 and int(config.RPC_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number rpc-port configuration parameter")

    # socket.io host
    if args.socketio_host:
        config.SOCKETIO_HOST = args.socketio_host
    elif has_config and configfile.has_option('Default', 'socketio-host'):
        config.SOCKETIO_HOST = configfile.get('Default', 'socketio-host')
    else:
        config.SOCKETIO_HOST = 'localhost'

    # socket.io port
    if args.socketio_port:
        config.SOCKETIO_PORT = args.socketio_port
    elif has_config and configfile.has_option('Default', 'socketio-port') and configfile.get('Default', 'socketio-port'):
        config.SOCKETIO_PORT = configfile.get('Default', 'socketio-port')
    else:
        config.SOCKETIO_PORT = '4101'
    try:
        int(config.SOCKETIO_PORT)
        assert int(config.SOCKETIO_PORT) > 1 and int(config.SOCKETIO_PORT) < 65535
    except:
        raise Exception("Please specific a valid port number socketio-port configuration parameter")

    # Log
    if args.log_file:
        config.LOG = args.log_file
    elif has_config and configfile.has_option('Default', 'logfile'):
        config.LOG = configfile.get('Default', 'logfile')
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
    mongo_client = pymongo.MongoClient(config.MONGODB_CONNECT, int(config.MONGODB_PORT))
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
    
    logging.info("Starting up ZeroMQ subscriber...")
    zmq_context = zmq.Context()
    gevent.spawn(zmq_subscriber, zmq_context)
    
    logging.info("Starting up socket.io server...")
    sio_server = socketio_server.SocketIOServer(
        (config.SOCKETIO_HOST, int(config.SOCKETIO_PORT)),
        SocketIOApp(zmq_context), resource="socket.io")        
    sio_server.start() #start the socket.io server greenlets

    logging.info("Starting up RPC API handler...")
    api.serve_api(db)

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
