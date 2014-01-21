Interacting with the API
=========================

.. warning::

    This API documentation is still in an early state. It contains errors, omissions, etc., and could change drastically at any time.

    
Overview
----------

``counterwalletd`` features a full-fledged JSON RPC-based API, which services Counterwallet, as well as any
3rd party wallets which wish to use it, by providing useful functions such as preferences storage, as well
as advanced time-series data retrieval and processing (useful for distributed exchange price data, and more).

Connecting to the API
----------------------

By default, ``counterwalletd`` will listen on port ``4001`` for API
requests. API requests are made via a HTTP POST request to ``/jsonrpc/``, with JSON-encoded
data passed as the POST body. For more information on JSON RPC, please see the `JSON RPC specification <http://json-rpc.org/wiki/specification>`__.


Terms & Conventions
---------------------

.. _walletid:

wallet IDs
^^^^^^^^^^^

An individual Counterwallet user needs a way to identify themselves to ``counterwalletd`` for things like storing
and retrieving their wallet preferences data, and more.

For this purpose, we define the concept of a wallet ID, which is simply the user's Counterwallet 12-word password,
hashed with SHA1, and the resulting hash hashed again with SHA1.


.. _read_api:

Read API Function Reference
------------------------------------

.. _get_preferences:

get_preferences
^^^^^^^^^^^^^^

.. py:function:: get_preferences(wallet_id)

   Gets the preferences for a given wallet ID.

   :param string wallet_id: The wallet ID to retrieve the preferences for.
   :return: A :ref:`wallet preferences object <wallet-preferences-object>` if the wallet ID had stored preferences, otherwise ``{}`` (empty object).



.. _action_api:

Action/Write API Function Reference
-----------------------------------

.. _store_preferences:

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

* **wallet_id** (*string*): The wallet user's :ref:`wallet ID <walletid>`.
* **num_addresses_used** (*integer*): The number of addresses utilized in the user's wallet (this
  determines how many addresses we will deterministally generate when the user logs in).
* **address_aliases** (*list*): A list of zero or objects, with each object having an ``address`` string property,
  being the Bitcoin base56 address, and an ``alias`` string property, being the textual alias (i.e. nickname)
  for this address. Using aliases helps make the wallet more user-friendly.
