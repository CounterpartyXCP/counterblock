'''
http://insight.bitpay.com/
'''
import logging

from lib import config, util, util_bitcoin

def get_host():
    if config.BLOCKCHAIN_SERVICE_CONNECT:
        return config.BLOCKCHAIN_SERVICE_CONNECT
    else:
        return 'http://localhost:3001' if config.TESTNET else 'http://localhost:3000'

def check():
    result = util.get_url(get_host() + '/api/sync/', abort_on_error=True)
    if not result:
        raise Exception('Insight reports error: %s' % result['error'])
    if result['status'] == 'error':
        raise Exception('Insight reports error: %s' % result['error'])
    if result['status'] == 'syncing':
        logging.warning("WARNING: Insight is not fully synced to the blockchain: %s%% complete" % result['syncPercentage'])

def getinfo():
    return util.get_url(get_host() + '/api/status?q=getInfo', abort_on_error=True)

def listunspent(address):
    return util.get_url(get_host() + '/api/addr/' + address + '/utxo/', abort_on_error=True)

def getaddressinfo(address):
    return util.get_url(get_host() + '/api/addr/' + address + '/', abort_on_error=True)

def gettransaction(tx_hash):
    return util.get_url(get_host() + '/api/tx/' + tx_hash + '/', abort_on_error=False)

def get_pubkey_for_address(address):
    #first, get a list of transactions for the address
    address_info = getaddressinfo(address)

    #if no transactions, we can't get the pubkey
    if not address_info['transactions']:
        return None
    
    #for each transaction we got back, extract the vin, pubkey, go through, convert it to binary, and see if it reduces down to the given address
    for tx_id in address_info['transactions']:
        #parse the pubkey out of the first sent transaction
        tx = gettransaction(tx_id)
        pubkey_hex = tx['vin'][0]['scriptSig']['asm'].split(' ')[1]
        if util_bitcoin.pubkey_to_address(pubkey_hex) == address:
            return pubkey_hex
    return None
