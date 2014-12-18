counterblockd
==============

Provides extended API functionality over counterpartyd (such as market information, asset history, etc). Works alongside counterpartyd.

Installation on Ubuntu
==============

Many of the requirements for counterblockd are similar to those for counterpartyd, so if you used the counterpartyd installer at https://github.com/CounterpartyXCP/counterpartyd_build to install, you already have many requirements installed.  One big thing that WON'T be included is MongoDB.

If you don't already have MongoDB, install it:

```bash
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/10gen.list
sudo apt-get update
sudo apt-get install mongodb-10gen
```

The last few things can be added by running:

```bash
~/counterblockd$ sudo apt-get install python-pip cython libxml2-dev libxslt1-dev python-dev
~/counterblockd$ sudo pip install -r pip-requirements.txt 
```