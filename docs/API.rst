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

* **num_addresses_used** (*integer*): The number of addresses utilized in the user's wallet (this
  determines how many addresses we will deterministally generate when the user logs in).
* **address_aliases** (*list*): A list of zero or objects, with each object having an ``address`` string property,
  being the Bitcoin base56 address, and an ``alias`` string property, being the textual alias (i.e. nickname)
  for this address. Using aliases helps make the wallet more user-friendly.
