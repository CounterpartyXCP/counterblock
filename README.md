counterblockd
==============

Provides extended API functionality over counterpartyd (such as market information, asset history, etc). Works alongside counterpartyd.

Installation
==============

Many of the requirements for counterblockd are similar to those for counterpartyd, so if you used the counterpartyd installer at https://github.com/CounterpartyXCP/counterpartyd_build to install, you already have many requirements installed.  The last couple can be added by running (on Ubuntu):

```bash
~/counterblockd$ sudo apt-get install python-pip cython libxml2-dev libxslt1-dev python-dev
~/counterblockd$ sudo pip install -r pip-requirements.txt 
```