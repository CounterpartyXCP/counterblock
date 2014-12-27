counterblockd
==============

Provides extended API functionality over counterpartyd (such as market information, asset history, etc).
Works alongside `counterpartyd`.

Automatic Installation
------------------------

Please see the [Setting up a Federated Node](http://counterparty.io/docs/build-system/federated-node/) guide
(under the *"Node Setup"* section) for a script that will set counterblockd up for you, automatically. When prompted
for the node type, choose "counterblockd basic".

Manual Installation on Ubuntu
---------------------------------

Many of the requirements for `counterblockd` are similar to those for `counterpartyd`, so if you used the
[counterpartyd installer](https://github.com/CounterpartyXCP/counterpartyd_build) to install, you already
have many requirements installed.

The first extra thing would be to install MongoDB:

```bash
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/10gen.list
sudo apt-get update
sudo apt-get install mongodb-10gen
```

The last couple can be added by running (from the `counterblockd` directory):

```bash
sudo apt-get install python-pip cython libxml2-dev libxslt1-dev python-dev
sudo pip install -r pip-requirements.txt 
```
