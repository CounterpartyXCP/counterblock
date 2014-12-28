counterblockd
==================================================

``counterblockd`` provides additional services to Counterwallet beyond those offered in the API provided by ``counterpartyd``.
It features a full-fledged JSON RPC-based API, which services Counterwallet, as well as any 3rd party services which wish to use it.

Such services include:

- Realtime data streaming via socket.io
- An extended API for Counterwallet-specific actions like wallet preferences storage and retrieval
- API includes functionality for retieving processed time-series data suitable for display and manipulation
  (useful for distributed exchange price data, and more)

``counterblockd`` has an extensible architecture, and developers may write custom plugins for it, which are loaded
dynamically and allow them to extend ``counterblockd`` with new parsing functionality, write gateways to other currencies
or services, and much more.

Contents:

.. toctree::
   :maxdepth: 3

   API
   Modules


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

