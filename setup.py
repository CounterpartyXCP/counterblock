#!/usr/bin/env python3

from setuptools import setup, find_packages
import os
import sys
import logging

from counterblock.lib import config

required_packages = [
    'appdirs',
    'prettytable',
    'python-dateutil',
    'flask',
    'json-rpc',
    'pytest',
    'pycoin',
    'python-bitcoinlib',
    'pymongo',
    'gevent',
    'gevent-websocket',
    'gevent-socketio',
    'grequests',
    'redis',
    'pyzmq',
    'pillow',
    'lxml',
    'jsonschema',
    'strict_rfc3339',
    'rfc3987',
    'aniso8601',
    'pygeoip',
    'colorama',
    'configobj',
    'repoze.lru'
]

setup_options = {
    'name': 'counterblock',
    'version': config.VERSION,
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
    'download_url': 'https://github.com/CounterpartyXCP/counterblock/releases/tag/%s' % config.VERSION,
    'provides': ['counterblock'],
    'packages': find_packages(),
    'zip_safe': False,
    'setup_requires': ['appdirs',],
    'install_requires': required_packages,
    'include_package_data': True,
    'entry_points': {
        'console_scripts': [
            'counterblock = counterblock:server_main',
        ]
    },
    'package_data': {
        'counterblock.schemas': ['asset.schema.json', 'feed.schema.json'],
    }
}

if os.name == "nt":
    sys.exit("Windows installs not supported")

setup(**setup_options)
