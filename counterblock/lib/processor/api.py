import os
import json
import re
import time
import datetime
import base64
import decimal
import operator
import logging
import copy
import urllib.request
import urllib.parse
import urllib.error
import functools
from logging import handlers as logging_handlers
import calendar

from gevent import wsgi
import grequests
import flask
import jsonrpc
import pymongo

from counterblock.lib import config, cache, database, util, blockchain, blockfeed, messages
from counterblock.lib.processor import API

API_MAX_LOG_SIZE = 10 * 1024 * 1024  # max log size of 20 MB before rotation (make configurable later)
API_MAX_LOG_COUNT = 10

decimal.setcontext(decimal.Context(prec=8, rounding=decimal.ROUND_HALF_EVEN))
D = decimal.Decimal
logger = logging.getLogger(__name__)


def serve_api():
    # Preferneces are just JSON objects... since we don't force a specific form to the wallet on
    # the server side, this makes it easier for 3rd party wallets (i.e. not Counterwallet) to fully be able to
    # use counterblockd to not only pull useful data, but also load and store their own preferences, containing
    # whatever data they need

    app = flask.Flask(__name__)
    assert config.mongo_db
    tx_logger = logging.getLogger("transaction_log")  # get transaction logger

    @API.add_method
    def get_messagefeed_messages_by_index(message_indexes):
        msgs = util.call_jsonrpc_api("get_messages_by_index", {'message_indexes': message_indexes}, abort_on_error=True)['result']
        events = []
        for m in msgs:
            events.append(messages.decorate_message_for_feed(m))
        return events

    @API.add_method
    def get_chain_block_height():
        # DEPRECIATED 1.5
        return config.state['cp_backend_block_index']

    @API.add_method
    def get_insight_block_info(block_hash):
        info = blockchain.getBlockInfo(block_hash)  # ('/api/block/' + block_hash + '/', abort_on_error=True)
        return info

    @API.add_method
    def get_chain_address_info(addresses, with_uxtos=True, with_last_txn_hashes=4):
        if not isinstance(addresses, list):
            raise Exception("addresses must be a list of addresses, even if it just contains one address")
        results = []

        for address in addresses:
            info = blockchain.getaddressinfo(address)
            txns = info['transactions']
            del info['transactions']
            result = {}
            result['addr'] = address
            result['info'] = info
            result['block_height'] = config.state['cp_backend_block_index']
            #^ yeah, hacky...it will be the same block height for each address (we do this to avoid an extra API call to get_block_height)
            if with_uxtos:
                result['uxtos'] = blockchain.listunspent(address)
            if with_last_txn_hashes:
                result['last_txns'] = txns
            results.append(result)

        return results

    @API.add_method
    def get_optimal_fee_per_kb():
        fees = cache.get_value("FEE_PER_KB")
        if not fees:
            if config.BLOCKTRAIL_API_KEY:
                # query blocktrail API
                fees = util.get_url(
                    "https://api.blocktrail.com/v1/BTC/fee-per-kb?api_key={}".format(config.BLOCKTRAIL_API_KEY),
                    abort_on_error=True, is_json=True)
            else:
                # query bitcoind
                fees = {}
                fees['optimal'] = util.call_jsonrpc_api("fee_per_kb", {'conf_target': 3}, abort_on_error=True, use_cache=False)['result']
                fees['low_priority'] = util.call_jsonrpc_api("fee_per_kb", {'conf_target': 8}, abort_on_error=True, use_cache=False)['result']
            cache.set_value("FEE_PER_KB", fees, cache_period=60 * 5)  # cache for 5 minutes
        return fees

    @API.add_method
    def get_chain_txns_status(txn_hashes):
        if not isinstance(txn_hashes, list):
            raise Exception("txn_hashes must be a list of txn hashes, even if it just contains one hash")
        results = []
        tx_info = blockchain.gettransaction_batch(txn_hashes)
        for tx_hash in txn_hashes:
            assert tx_hash in tx_info
            assert tx_info[tx_hash] is None or tx_info[tx_hash]['txid'] == tx_hash
            if tx_info[tx_hash] is not None:  # don't append anything to results list if the result is None
                results.append({
                    'tx_hash': tx_info[tx_hash]['txid'],
                    'blockhash': tx_info[tx_hash].get('blockhash', None),  # not provided if not confirmed on network
                    'confirmations': tx_info[tx_hash].get('confirmations', 0),  # not provided if not confirmed on network
                    'blocktime': tx_info[tx_hash].get('time', None),
                })
        return results

    @API.add_method
    def get_last_n_messages(count=100):
        if count > 1000:
            raise Exception("The count is too damn high")
        message_indexes = list(range(max(config.state['last_message_index'] - count, 0) + 1, config.state['last_message_index'] + 1))
        msgs = util.call_jsonrpc_api(
            "get_messages_by_index", {'message_indexes': message_indexes}, abort_on_error=True)['result']
        for i in range(len(msgs)):
            msgs[i] = messages.decorate_message_for_feed(msgs[i])
        return msgs

    @API.add_method
    def get_pubkey_for_address(address):
        # returns None if the address has made 0 transactions (as we wouldn't be able to get the public key)
        return blockchain.get_pubkey_for_address(address) or False

    @API.add_method
    def get_script_pub_key(tx_hash, vout_index):
        tx = blockchain.gettransaction(tx_hash)
        if tx is not None and 'vout' in tx and len(tx['vout']) > vout_index:
            return tx['vout'][vout_index]
        return None

    @API.add_method
    def broadcast_tx(signed_tx_hex):
        return blockchain.broadcast_tx(signed_tx_hex)

    @API.add_method
    def get_raw_transactions(address, start_ts=None, end_ts=None, limit=1000):
        """Gets raw transactions for a particular address

        @param address: A single address string
        @param start_ts: The starting date & time. Should be a unix epoch object. If passed as None, defaults to 60 days before the end_date
        @param end_ts: The ending date & time. Should be a unix epoch object. If passed as None, defaults to the current date & time
        @param limit: the maximum number of transactions to return; defaults to one thousand
        @return: Returns the data, ordered from newest txn to oldest. If any limit is applied, it will cut back from the oldest results
        """
        def get_address_history(address, start_block=None, end_block=None):
            address_dict = {}

            address_dict['balances'] = util.call_jsonrpc_api(
                "get_balances", {'filters': [{'field': 'address', 'op': '==', 'value': address}, ],
                                 }, abort_on_error=True)['result']

            address_dict['debits'] = util.call_jsonrpc_api(
                "get_debits",
                {'filters': [{'field': 'address', 'op': '==', 'value': address},
                             {'field': 'quantity', 'op': '>', 'value': 0}],
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['credits'] = util.call_jsonrpc_api(
                "get_credits",
                {'filters': [{'field': 'address', 'op': '==', 'value': address},
                             {'field': 'quantity', 'op': '>', 'value': 0}],
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['burns'] = util.call_jsonrpc_api(
                "get_burns",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}, ],
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['sends'] = util.call_jsonrpc_api(
                "get_sends",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}, {'field': 'destination', 'op': '==', 'value': address}],
                 'filterop': 'or',
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']
            #^ with filterop == 'or', we get all sends where this address was the source OR destination

            address_dict['sweeps'] = util.call_jsonrpc_api(
                "get_sweeps",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}, {'field': 'destination', 'op': '==', 'value': address}],
                 'filterop': 'or',
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['dispensers'] = util.call_jsonrpc_api(
                "get_sweeps",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}],
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['orders'] = util.call_jsonrpc_api(
                "get_orders",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}, ],
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['order_matches'] = util.call_jsonrpc_api(
                "get_order_matches",
                {'filters': [{'field': 'tx0_address', 'op': '==', 'value': address}, {'field': 'tx1_address', 'op': '==', 'value': address}, ],
                 'filterop': 'or',
                 'order_by': 'tx0_block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['btcpays'] = util.call_jsonrpc_api(
                "get_btcpays",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}, {'field': 'destination', 'op': '==', 'value': address}],
                 'filterop': 'or',
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['issuances'] = util.call_jsonrpc_api(
                "get_issuances",
                {'filters': [{'field': 'issuer', 'op': '==', 'value': address}, {'field': 'source', 'op': '==', 'value': address}],
                 'filterop': 'or',
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['broadcasts'] = util.call_jsonrpc_api(
                "get_broadcasts",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}, ],
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['bets'] = util.call_jsonrpc_api(
                "get_bets",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}, ],
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['bet_matches'] = util.call_jsonrpc_api(
                "get_bet_matches",
                {'filters': [{'field': 'tx0_address', 'op': '==', 'value': address}, {'field': 'tx1_address', 'op': '==', 'value': address}, ],
                 'filterop': 'or',
                 'order_by': 'tx0_block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['dividends'] = util.call_jsonrpc_api(
                "get_dividends",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}, ],
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['cancels'] = util.call_jsonrpc_api(
                "get_cancels",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}, ],
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['bet_expirations'] = util.call_jsonrpc_api(
                "get_bet_expirations",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}, ],
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['order_expirations'] = util.call_jsonrpc_api(
                "get_order_expirations",
                {'filters': [{'field': 'source', 'op': '==', 'value': address}, ],
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['bet_match_expirations'] = util.call_jsonrpc_api(
                "get_bet_match_expirations",
                {'filters': [{'field': 'tx0_address', 'op': '==', 'value': address}, {'field': 'tx1_address', 'op': '==', 'value': address}, ],
                 'filterop': 'or',
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']

            address_dict['order_match_expirations'] = util.call_jsonrpc_api(
                "get_order_match_expirations",
                {'filters': [{'field': 'tx0_address', 'op': '==', 'value': address}, {'field': 'tx1_address', 'op': '==', 'value': address}, ],
                 'filterop': 'or',
                 'order_by': 'block_index',
                 'order_dir': 'asc',
                 'start_block': start_block,
                 'end_block': end_block,
                 }, abort_on_error=True)['result']
            return address_dict

        now_ts = calendar.timegm(time.gmtime())
        if not end_ts:  # default to current datetime
            end_ts = now_ts
        if not start_ts:  # default to epoch
            start_ts = 0
        start_block_index, end_block_index = database.get_block_indexes_for_dates(
            start_dt=datetime.datetime.utcfromtimestamp(start_ts),
            end_dt=datetime.datetime.utcfromtimestamp(end_ts) if now_ts != end_ts else None)

        # make API call to counterparty-server to get all of the data for the specified address
        txns = []
        d = get_address_history(address, start_block=start_block_index, end_block=end_block_index)
        # mash it all together
        for category, entries in d.items():
            if category in ['balances', ]:
                continue
            for e in entries:
                e['_category'] = category
                e = messages.decorate_message(e, for_txn_history=True)  # DRY
            txns += entries
        txns = util.multikeysort(txns, ['-_block_time', '-_tx_index'])
        txns = txns[0:limit]  # TODO: we can trunk before sorting. check if we can use the messages table and use sql order and limit
        #^ won't be a perfect sort since we don't have tx_indexes for cancellations, but better than nothing
        # txns.sort(key=operator.itemgetter('block_index'))
        return txns

    @API.add_method
    def proxy_to_counterpartyd(method='', params=[]):
        if method == 'sql':
            raise Exception("Invalid method")
        result = None
        cache_key = None

        result = util.call_jsonrpc_api(method, params)

        if 'error' in result:
            if result['error'].get('data', None):
                errorMsg = result['error']['data'].get('message', result['error']['message'])
                if isinstance(errorMsg, bytes):
                    errorMsg = str(errorMsg, 'utf-8')
            else:
                errorMsg = json.dumps(result['error'])
            raise Exception(errorMsg if errorMsg is not None else "UNKNOWN")
        return result['result']

    def _set_cors_headers(response):
        if config.RPC_ALLOW_CORS:
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'DNT,X-Mx-ReqToken,Keep-Alive,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type'

    @app.route('/healthz', methods=['GET'])
    def handle_healthz():
        msg, code = 'Unhealthy', 503
        if blockfeed.fuzzy_is_caught_up():
            msg, code = 'Healthy', 200
        return flask.Response(msg, code, mimetype='text/plain')

    @app.route('/', methods=["OPTIONS", ])
    @app.route('/api/', methods=["OPTIONS", ])
    def handle_options():
        response = flask.Response('', 204)
        _set_cors_headers(response)
        return response

    @app.route('/', methods=["GET", ])
    @app.route('/api/', methods=["GET", ])
    def handle_get():
        if flask.request.headers.get("Content-Type", None) == 'application/csp-report':
            try:
                data_json = flask.request.get_data().decode('utf-8')
                data = json.loads(data_json)
                assert 'csp-report' in data
            except Exception as e:
                obj_error = jsonrpc.exceptions.JSONRPCInvalidRequest(data="Invalid JSON-RPC 2.0 request format")
                return flask.Response(obj_error.json.encode(), 200, mimetype='application/json')

            tx_logger.info("***CSP SECURITY --- %s" % data_json)
            return flask.Response('', 200)

        #"ping" counterpartyd to test
        cp_s = time.time()
        cp_result_valid = True
        try:
            cp_status = util.call_jsonrpc_api("get_running_info", abort_on_error=True, use_cache=False)['result']
        except:
            cp_result_valid = False
        cp_e = time.time()

        #"ping" counterblockd to test, as well
        cb_s = time.time()
        cb_result_valid = True
        cb_result_error_code = None
        payload = {
            "id": 0,
            "jsonrpc": "2.0",
            "method": "is_ready",
            "params": [],
        }
        headers = {'content-type': 'application/json', 'Connection': 'close'}
        try:
            url = "http://127.0.0.1:%s/api/" % config.RPC_PORT
            r = grequests.map((grequests.post(url, data=json.dumps(payload), headers=headers),))[0]
            if r is None:
                raise Exception("result is None")
        except Exception as e:
            cb_result_valid = False
            cb_result_error_code = "GOT EXCEPTION: %s" % e
        else:
            if r.status_code != 200:
                cb_result_valid = False
                cb_result_error_code = "GOT STATUS %s" % r.status_code if r else 'COULD NOT CONTACT'
            cb_result = r.json()
            if 'error' in cb_result:
                cb_result_valid = False
                cb_result_error_code = "GOT ERROR: %s" % cb_result['error']
        cb_e = time.time()

        result = {
            'counterparty-server': 'OK' if cp_result_valid else 'NOT OK',
            'counterparty-server_ver': '%s.%s.%s' % (
                cp_status['version_major'], cp_status['version_minor'], cp_status['version_revision']) if cp_result_valid else '?',
            'counterparty-server_last_block': cp_status['last_block'] if cp_result_valid else '?',
            'counterparty-server_last_message_index': cp_status['last_message_index'] if cp_result_valid else '?',
            'counterparty-server_caught_up': config.state['cp_caught_up'],
            'counterparty-server_check_elapsed': cp_e - cp_s,

            'counterblock': 'OK' if cb_result_valid else 'NOT OK',
            'counterblock_ver': config.VERSION,
            'counterblock_check_elapsed': cb_e - cb_s,
            'counterblock_error': cb_result_error_code,
            'counterblock_last_message_index': config.state['last_message_index'],
            'counterblock_caught_up': blockfeed.fuzzy_is_caught_up(),
            'counterblock_cur_block': {'block_hash': config.state['cur_block'].get('block_hash', '??'),
                                       'block_index': config.state['cur_block'].get('block_index', '??')},
            'counterblock_last_processed_block': {'block_hash': config.state['my_latest_block'].get('block_hash', '??'),
                                                  'block_index': config.state['my_latest_block'].get('block_index', '??')},
        }

        response_code = 200
        # error if we couldn't make a successful call to counterparty-server or counterblock's API (500)
        if not cp_result_valid or not cb_result_valid:
            response_code = 500
            result['ERROR'] = "counterparty-server_api_contact_error"
        # error 510 if the counterparty-server last block is more than 1 block behind backend
        elif not result['counterparty-server_caught_up']:
            response_code = 510
            result['ERROR'] = "counterparty-server_not_caught_up"
        # error 511 if the counterblock last block is more than 1 block behind counterparty-server
        elif not result['counterblock_caught_up']:
            response_code = 511
            result['ERROR'] = "counterblock_not_caught_up"
        else:
            result['ERROR'] = None

        response = flask.Response(json.dumps(result), response_code, mimetype='application/json')
        _set_cors_headers(response)
        return response

    @app.route('/', methods=["POST", ])
    @app.route('/api/', methods=["POST", ])
    def handle_post():
        # don't do anything if we're not caught up
        if not blockfeed.fuzzy_is_caught_up():
            obj_error = jsonrpc.exceptions.JSONRPCServerError(data="Server is not caught up. Please try again later.")
            response = flask.Response(obj_error.json.encode(), 525, mimetype='application/json')
            #^ 525 is a custom response code we use for this one purpose
            _set_cors_headers(response)
            return response

        try:
            request_json = flask.request.get_data().decode('utf-8')
            request_data = json.loads(request_json)
            assert 'id' in request_data and request_data['jsonrpc'] == "2.0" and request_data['method']
            # params may be omitted
        except:
            obj_error = jsonrpc.exceptions.JSONRPCInvalidRequest(data="Invalid JSON-RPC 2.0 request format")
            response = flask.Response(obj_error.json.encode(), 200, mimetype='application/json')
            _set_cors_headers(response)
            return response

        # only arguments passed as a dict are supported
        if request_data.get('params', None) and not isinstance(request_data['params'], dict):
            obj_error = jsonrpc.exceptions.JSONRPCInvalidRequest(
                data='Arguments must be passed as a JSON object (list of unnamed arguments not supported)')
            response = flask.Response(obj_error.json.encode(), 200, mimetype='application/json')
            _set_cors_headers(response)
            return response
        rpc_response = jsonrpc.JSONRPCResponseManager.handle(request_json, API)
        rpc_response_json = json.dumps(rpc_response.data, default=util.json_dthandler).encode()

        # log the request data
        try:
            assert 'method' in request_data
            tx_logger.debug("TRANSACTION --- %s ||| REQUEST: %s ||| RESPONSE: %s" % (request_data['method'], request_json, rpc_response_json))
        except Exception as e:
            logger.info("Could not log transaction: Invalid format: %s" % e)

        response = flask.Response(rpc_response_json, 200, mimetype='application/json')
        _set_cors_headers(response)
        return response

    # make a new RotatingFileHandler for the access log.
    api_logger = logging.getLogger("api_log")
    h = logging_handlers.RotatingFileHandler(
        os.path.join(config.log_dir, "server%s.api.log" % config.net_path_part),
        'a', API_MAX_LOG_SIZE, API_MAX_LOG_COUNT)
    api_logger.setLevel(logging.INFO)
    api_logger.addHandler(h)
    api_logger.propagate = False

    # hack to allow wsgiserver logging to use python logging module...
    def trimlog(log, msg):
        log.info(msg.rstrip())
    api_logger.write = functools.partial(trimlog, api_logger)

    # start up the API listener/handler
    server = wsgi.WSGIServer((config.RPC_HOST, int(config.RPC_PORT)), app, log=api_logger)
    server.serve_forever()
