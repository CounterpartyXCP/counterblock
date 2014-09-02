.. default-domain:: py

Interacting with the API
=========================

.. warning::

    This API documentation is INCOMPLETE. It contains errors, omissions, etc., and could change drastically at any time.

    
Connecting to the API
----------------------

By default, ``counterblockd`` will listen on port ``4001`` for API
requests. API requests are made via a HTTP POST request to ``/api/``, with JSON-encoded
data passed as the POST body. For more information on JSON RPC, please see the `JSON RPC specification <http://json-rpc.org/wiki/specification>`__.


Terms & Conventions
---------------------

.. _walletid:

wallet IDs
^^^^^^^^^^^

An individual Counterwallet user needs a way to identify themselves to ``counterblockd`` for things like storing
and retrieving their wallet preferences data, and more.

For this purpose, we define the concept of a wallet ID, which is simply the user's Counterwallet 12-word password,
double-hashed with SHA256 and converted to base 64.


.. _read_api:

Read API Function Reference
------------------------------------

Asset Functions
^^^^^^^^^^^^^^^

.. function:: get_base_quote_asset(asset1, asset2)

  Given two arbitrary assets, returns the base asset and the quote asset.

  :param asset1: An asset
  :param asset2: An asset
  :return Array:
  :rtype: {'base_asset','quote_asset','pair_name'}

.. function:: get_escrowed_balance(addresses)

  :param list addresses: List of addresses to check
  :return: FIXME

.. function:: get_market_cap_history(start_ts=None, end_ts=None)

  :param start_ts: Unix timestamp
  :param end_ts: Unix timestamp
  :return: Array
  :rtype: {'base_currency':[{'data':[ts,market_cap], 'name'}]}

.. function:: get_market_info(assets)

  :param list assets: Assets to check
  :return: Array
  :rtype: {'24h_hlc_in_btc','extended_description','extended_pgpsig','aggregated_price_as_btc','price_in_btc','24h_summary':{'vol','count'}, 'market_cap_in_btc','asset','price_as_xcp', '7d_history_in_btc':[[ts, price]], '24h_vol_price_change_in_xcp','price_in_xcp','extended_website','24h_vol_price_change_in_btc','aggregated_price_as_xcp','market_cap_in_xcp','7d_history_in_xcp':[[ts, price]],'aggregated_price_in_btc','aggregated_price_in_xcp','price_as_btc','total_supply','24h_ohlc_xcp','extended_image'}

.. function:: get_market_info_leaderboard(limit=100)

  :param limit: Number of results to return
  :return: Array
  :rtype: {base_currency:[{
                           '24h_ohlc_in_btc'
                           'total_supply'
                           'aggregated_price_in_btc'
                           'price_in_btc'
                           '24h_vol_price_change_in_xcp'
                           'aggregated_price_in_xcp'
                           '24h_summary: {'vol','count'}
                           'price_in_xcp'
                           'price_as_btc'
                           'market_cap_in_btc'
                           '24h_ohlc_in_xcp'
                           '24h_vol_price_change_in_btc'
                           'aggregated_price_as_xcp'
                           'market_cap_in_xcp'
                           'asset'
                           'price_as_xcp'
                           '7d_history_in_xcp'
                           '7d_history_in_btc'
                           'aggregated_price_as_btc'}]}

.. function:: get_market_price_summary(asset1, asset2, with_last_trades=0)

  :param asset1: An asset
  :param asset2: An asset
  :param with_last_trades: Include last trades
  :return Array:
  :rtype: {'quote_asset','base_asset','market_price',('last_trades')}

.. function:: get_normalized_balances(addresses)

  This call augments counterpartyd's get_balances with a normalized_quantity field. It also will include any owned assets for an address, even if their balance is zero. NOTE: Does not retrieve BTC balance. Use get_address_info for that.

  :param list addresses: List of addresses to check
  :return: List
  :rtype: [{'address','asset','quantity','normalized_quantity','owner'}]



Debugging Functions
^^^^^^^^^^^^^^^^^^^

.. function:: get_reflected_host_info()

  Allows the requesting host to get some info about itself, such as its IP. Used for troubleshooting.

  :return: Client host info
  :rtype: {'ip','cookie','country'}

Blockchain Functions
^^^^^^^^^^^^^^^^^^^^

.. function:: get_chain_address_info(addresses, with_uxtos=True, with_last_txn_hashes=4, with_block_height=False)

  Get info for one or more addresses

  :parameter list addresses: Address to query
  :parameter boolean with_uxtos: Include Unspent
  :parameter int with_last_txn_hashes: Include n recent confirmed transactions
  :param boolean with_block_height: Include block height
  :return: Address info
  :rtype: [{'addr','info',('uxto'),('last_txns'),('block_height')}]

.. function:: get_chain_block_height()

  :return: The height of the block chain

.. function get_chain_txns_status(txn_hashes)
  :param list txn_hashes: A list of one or more txn hashes
  :return: Transaction information
  :rtype: [{'tx_hash','blockhash','confirmations','blocktime'}]



Message Functions
^^^^^^^^^^^^^^^^^

.. function:: get_last_n_messages(count=100)

  Return latest messaages

  :param int count: Number of messages to return. Must be < 1000 if specified.
  :return: A list of messages
  :rtype: [{'raw_tx_type', ... other fields vary per tx type}]

.. function:: get_messagefeed_messages_by_index(message_indexes)

  Alias for counterpartyd get_messages_by_index

  :param list message_indexs: Message IDs to fetch
  :return: A list of messages

Transaction Functions
^^^^^^^^^^^^^^^^^^^^^

.. function:: get_raw_transactions(address, start_ts=None, end_ts=None, limit=500):

      Gets raw transactions for a particular address

      :param address: A single address string
      :param start_ts: The starting date & time. Should be a unix epoch object. If passed as None, defaults to 60 days before the end_date
      :param end_ts: The ending date & time. Should be a unix epoch object. If passed as None, defaults to the current date & time
      :param limit: the maximum number of transactions to return; defaults to ten thousand
      :return: Returns the data, ordered from newest txn to oldest. If any limit is applied, it will cut back from the oldest results
      :rtype: {id: {status, tx_hash, _divisible, _tx_index, block_index, _category, destination, tx_index, _block_time, source, asset, _command, quantity}}


Action/Write API Function Reference
-----------------------------------


store_preferences
^^^^^^^^^^^^^^^^^^

.. py:function:: store_preferences(wallet_id, preferences)

   Stores the preferences for a given wallet ID.

   :param string wallet_id: The wallet ID to store the preferences for.
   :param object preferences: A :ref:`wallet preferences object <wallet-preferences-object>`
   :return: ``true`` if the storage was successful, ``false`` otherwise.



Objects
----------

The API calls documented can return any one of these objects.


.. _wallet-preferences-object:

Wallet Preferences Object
^^^^^^^^^^^^^^^^^^^^^^^^^^

An object that stores the Counterwallet preferences for the given wallet ID.

* **num_addresses_used** (*integer*): The number of addresses utilized in the user's wallet (this
  determines how many addresses we will deterministally generate when the user logs in).
* **address_aliases** (*list*): A list of zero or objects, with each object having an ``address`` string property,
  being the Bitcoin base56 address, and an ``alias`` string property, being the textual alias (i.e. nickname)
  for this address. Using aliases helps make the wallet more user-friendly.
