#! /usr/bin/env python3
"""
counterblockd server
"""

# import before importing other modules
import gevent
from gevent import monkey
# now, import grequests as it will perform monkey_patch_all()
# letting grequests do it avoids us double monkey patching... (ugh...)
import grequests  # this will monkey patch
if not monkey.is_module_patched("os"):  # if this fails, it's because gevent stopped monkey patching for us
    monkey.patch_all()
# disable urllib3 warnings for requests module (for now at least)
# (note that requests is used/imported through grequests only)
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import sys
import os
import argparse
import json
import logging
import datetime
import time
import tempfile

from counterblock.lib import config, config_util, log, blockfeed, util, module, database
from counterblock.lib.processor import messages, caughtup, startup  # to kick off processors
from counterblock.lib.processor import StartUpProcessor

logger = logging.getLogger(__name__)

CONFIG_ARGS = [
    # BASIC FLAGS
    [('-v', '--verbose'), {'dest': 'verbose', 'action': 'store_true', 'default': False, 'help': 'sets log level to DEBUG instead of WARNING'}],
    [('--testnet',), {'action': 'store_true', 'default': False, 'help': 'use {} testnet addresses and block numbers'.format(config.BTC_NAME)}],
    [('--regtest',), {'action': 'store_true', 'default': False, 'help': 'use {} regtest addresses and block numbers'.format(config.BTC_NAME)}],
    [('--log-file',), {'nargs': '?', 'const': None, 'default': False, 'help': 'log to the specified file (specify option without filename to use the default location)'}],
    [('--log-size-kb',), {'help': 'maximum log file size, in kilobytes'}],
    [('--log-num-files',), {'help': 'maximum number of rotated log files'}],
    [('--tx-log-file',), {'nargs': '?', 'const': None, 'default': False, 'help': 'log transaction info to the specified file (specify option without filename to use the default location)'}],
    [('--pid-file',), {'help': 'the location of the pid file'}],

    # THINGS WE CONNECT TO
    [('--backend-connect',), {'help': 'the hostname or IP of the backend bitcoind JSON-RPC server'}],
    [('--backend-port',), {'type': int, 'help': 'the backend JSON-RPC port to connect to'}],
    [('--backend-user',), {'help': 'the username used to communicate with backend over JSON-RPC'}],
    [('--backend-password',), {'help': 'the password used to communicate with backend over JSON-RPC'}],

    [('--counterparty-connect',), {'help': 'the hostname of the counterpartyd JSON-RPC server'}],
    [('--counterparty-port',), {'type': int, 'help': 'the port used to communicate with counterpartyd over JSON-RPC'}],
    [('--counterparty-user',), {'help': 'the username used to communicate with counterpartyd over JSON-RPC'}],
    [('--counterparty-password',), {'help': 'the password used to communicate with counterpartyd over JSON-RPC'}],

    [('--mongodb-connect',), {'help': 'the hostname of the mongodb server to connect to'}],
    [('--mongodb-port',), {'type': int, 'help': 'the port used to communicate with mongodb'}],
    [('--mongodb-database',), {'help': 'the mongodb database to connect to'}],
    [('--mongodb-user',), {'help': 'the optional username used to communicate with mongodb'}],
    [('--mongodb-password',), {'help': 'the optional password used to communicate with mongodb'}],

    [('--redis-connect',), {'help': 'the hostname of the redis server to use for caching (if enabled'}],
    [('--redis-port',), {'type': int, 'help': 'the port used to connect to the redis server for caching (if enabled)'}],
    [('--redis-database',), {'type': int, 'help': 'the redis database ID (int) used to connect to the redis server for caching (if enabled)'}],

    [('--blocktrail-api-key',), {'help': 'specify a valid blocktrail API key to allow for better fee estimation'}],
    [('--blocktrail-api-secret',), {'help': 'specify a valid blocktrail API secret to allow for better fee estimation'}],

    # COUNTERBLOCK API
    [('--rpc-host',), {'help': 'the IP of the interface to bind to for providing JSON-RPC API access (0.0.0.0 for all interfaces)'}],
    [('--rpc-port',), {'type': int, 'help': 'port on which to provide the counterblockd JSON-RPC API'}],
    [('--rpc-allow-cors',), {'action': 'store_true', 'default': True, 'help': 'Allow ajax cross domain request'}],
]


def main():
    # Post installation tasks
    config_util.generate_config_files()

    # Parse command-line arguments.
    parser = argparse.ArgumentParser(prog='counterblock', description='counterblock daemon')

    parser.add_argument(
        '-V', '--version', action='version', version="counterblock %s" % config.VERSION)
    parser.add_argument('--config-file', help='the path to the configuration file')
    config_util.add_config_arguments(parser, CONFIG_ARGS, 'server.conf')

    # actions
    subparsers = parser.add_subparsers(dest='command', help='the action to be taken')
    subparsers.required = True
    parser_server = subparsers.add_parser('server', help='Run counterblock')
    parser_reparse = subparsers.add_parser('reparse', help='Reparse the counterblock database')
    parser_enmod = subparsers.add_parser('enmod', help='Enable a module')
    parser_enmod.add_argument('module_path', type=str, help='Full Path of module to Enable relative to Counterblockd directory')
    parser_dismod = subparsers.add_parser('dismod', help='Disable a module')
    parser_dismod.add_argument('module_path', type=str, help='Path of module to Disable relative to Counterblockd directory')
    parser_listmod = subparsers.add_parser('listmod', help='Display Module Config')
    parser_rollback = subparsers.add_parser('rollback', help='Rollback to a specific block number')
    parser_rollback.add_argument('block_index', type=int, help='Block index to roll back to')

    args = parser.parse_args()

    config.init(args)

    log.set_up(args.verbose)

    # log unhandled errors.
    def handle_exception(exc_type, exc_value, exc_traceback):
        logger.error("Unhandled Exception", exc_info=(exc_type, exc_value, exc_traceback))
    sys.excepthook = handle_exception

    # Create/update pid file
    pid = str(os.getpid())
    pidf = open(config.PID, 'w')
    pidf.write(pid)
    pidf.close()

    # load any 3rd party modules
    module.load_all()

    # Handle arguments
    logger.info("counterblock command specified: {}".format(args.command))
    if args.command == 'enmod':
        module.toggle(args.module_path, True)
        sys.exit(0)
    elif args.command == 'dismod':
        module.toggle(args.module_path, False)
        sys.exit(0)
    elif args.command == 'listmod':
        module.list_all()
        sys.exit(0)
    elif args.command == 'reparse':
        startup.init_mongo()
        database.init_reparse(quit_after=True)
    elif args.command == 'rollback':
        assert args.block_index >= 1
        startup.init_mongo()
        database.rollback(args.block_index)
        sys.exit(0)

    assert args.command in ('server', 'reparse')
    logger.info("counterblock Version %s starting ..." % config.VERSION)

    # Run Startup Functions
    StartUpProcessor.run_active_functions()

if __name__ == '__main__':
    main()
