[![Slack Status](http://slack.counterparty.io/badge.svg)](http://slack.counterparty.io)

counterblock
==============

Provides extended API functionality over `counterparty-server` (such as market information, asset history, etc).
Works alongside `counterparty`.

# Installation

For a simple Docker-based install of the Counterparty software stack, see [this guide](http://counterparty.io/docs/federated_node/).

# Manual installation

(Linux only.) First, install `mongodb` and `redis`, and have an instance of `bitcoind` (`addrindex` branch) and [`counterparty-server`](https://github.com/CounterpartyXCP/counterparty-lib) running.

Then, download and install `counterblock`:

```
$ git clone https://github.com/CounterpartyXCP/counterblock.git
$ cd counterblock
$ sudo pip3 install -r requirements.txt
$ sudo python3 setup.py install
```

Then, launch the daemon via the following command, with the passwords set as appropriate:

```
$ counterblock --backend-password=rpc --counterparty-password=rpc server
```

Further command line options are available via:

* `$ counterblock --help`
