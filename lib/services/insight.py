'''
http://insight.bitpay.com/
'''
from lib import (config, util)

def check():
    result = util.call_blockchain_api('/api/sync/', abort_on_error=True)
    if not result:
        raise Exception('Insight reports error: %s' % result['error'])
    if result['status'] == 'error':
        raise Exception('Insight reports error: %s' % result['error'])
    if result['status'] == 'syncing':
        logging.warning("WARNING: Insight is not fully synced to the blockchain: %s%% complete" % result['syncPercentage'])

def getinfo():
    return util.call_blockchain_api('/api/status?q=getInfo', abort_on_error=True)

def listunspent(address):
    return util.call_blockchain_api('/api/addr/' + address + '/utxo/', abort_on_error=True)

def getaddressinfo(address):
    return util.call_blockchain_api('/api/addr/' + address + '/', abort_on_error=True)

def gettransaction(tx_hash):
    return util.call_blockchain_api('/api/tx/' + tx_hash + '/', abort_on_error=False)