counterblockd
==============

### __counterblockd__ currently uses __Python 2.7__ due to gevent-socketio's lack of support for Python 3
(geventhttpclient/_parser.cpython-32mu.so not load well with boost_python-py3* under Python 3).

Provides extended API functionality over counterpartyd (such as market information, asset history, etc). Works alongside counterpartyd.

See also: 

[fully automated installation script and installs ALL dependencies, including ``bitcoind`` and ``insight``](http://counterpartyd-build.readthedocs.org/en/latest/SettingUpAFederatedNode.html#node-setup)
[counterpartyd build-system](http://counterparty.io/docs/build-system/federated-node/)

