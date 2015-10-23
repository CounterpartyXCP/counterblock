#!/usr/bin/env python

from setuptools import setup, find_packages
import os
import sys
import logging

VERSION="1.2.0"

required_packages = [
    'appdirs>=1.4.0',
    'prettytable>=0.7.2',
    'python-dateutil>=2.4.0',
    'flask>=0.10.1',
    'json-rpc>=1.10.2',
    'pytest>=2.6.3',
    'pycoin>=0.52',
    'python-bitcoinlib>=0.2.1',
    'pymongo>=2.8,<3.0',
    'gevent<=1.0.2,<1.1.0',
    'gevent-websocket>=0.9.5',
    'gevent-socketio>=0.3.6',
    'geventhttpclient<=1.2.0',
    'redis>=2.10.3',
    'grequests<=0.2.1,<0.3.0',
    'pyzmq>=14.4.1',
    'pillow>=2.6.1',
    'lxml>=3.4.1',
    'jsonschema>=2.4.0',
    'strict_rfc3339>=0.5',
    'rfc3987>=1.3.4',
    'aniso8601>=0.82',
    'pygeoip>=0.3.2',
    'colorama>=0.3.2',
    'configobj>=5.0.6',
    'repoze.lru>=0.6'
]

required_repos = [
    #'https://github.com/robby-dermody/gevent/archive/e3cd7272178af7da0b85583cd2820f2520a8ba54.zip#egg=gevent-1.2',
    'https://github.com/gevent/gevent/archive/52924e3f68fff76c249dac7d867bae262e1196cc.zip#egg=gevent-1.2',    
    'https://github.com/gwik/geventhttpclient/archive/83ded6980a2e37025acbe5a93a52ceedd8f9338a.zip#egg=geventhttpclient-1.2.0',
    'https://github.com/natecode/grequests/archive/ea00e193074fc11d71b4ff74138251f6055ca364.zip#egg=grequests-0.2.1',
    #^ grequests (waiting until the next point release with natecode's pull request factored in)
]

setup_options = {
    'name': 'counterblock',
    'version': VERSION,
    'author': 'Counterparty Foundation',
    'author_email': 'support@counterparty.io',
    'maintainer': 'Counteparty Development Team',
    'maintainer_email': 'dev@counterparty.io',
    'url': 'http://counterparty.io',
    'license': 'MIT',
    'description': 'counterblock server',
    'long_description': 'Implements support for extended functionality for counterparty',
    'keywords': 'counterparty, bitcoin, counterblock, counterblockd',
    'classifiers': [
      "Programming Language :: Python",
    ],
    'download_url': 'https://github.com/CounterpartyXCP/counterblock/releases/tag/%s' % VERSION,
    'provides': ['counterblock'],
    'packages': find_packages(),
    'zip_safe': False,
    'dependency_links': required_repos,
    'setup_requires': ['appdirs==1.4.0','cython>=0.23.4'],
    'install_requires': required_packages,
    'include_package_data': True,
    'entry_points': {
        'console_scripts': [
            'counterblock = counterblock:server_main',
            'armory_utxsvr = counterblock:armory_utxsvr_main',
        ]
    },
    'package_data': {
        'counterblock.schemas': ['asset.schema.json', 'feed.schema.json'],
    }
}

if os.name == "nt":
    sys.exit("Windows installs not supported")

setup(**setup_options)
