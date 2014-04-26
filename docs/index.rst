counterblockd
==================================================

``counterblockd`` features a full-fledged JSON RPC-based API, which services Counterwallet, as well as any
3rd party services which wish to use it.

``counterblockd`` provides additional services to Counterwallet beyond those offered in the API provided by ``counterpartyd``.

Such services include:

- Realtime data streaming via socket.io
- An extended API for Counterwallet-specific actions like wallet preferences storage and retrieval
- API includes functionality for retieving processed time-series data suitable for display and manipulation
  (useful for distributed exchange price data, and more)

Contents:

.. toctree::
   :maxdepth: 2

   API


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

