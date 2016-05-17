#!/usr/bin/env python3

from setuptools import setup, find_packages
import os
import sys
import logging

from counterblock.lib import config

class generate_configuration_files(Command):
    description = "Generate configfiles from old counterparty-server and/or bitcoind config files"
    user_options = []

    def initialize_options(self):
        pass
    def finalize_options(self):
        pass

    def run(self):
        from counterpartycli.setup import generate_config_files
        generate_config_files()

class install(_install):
    description = "Install counterblock and dependencies"

    def run(self):
        caller = sys._getframe(2)
        caller_module = caller.f_globals.get('__name__','')
        caller_name = caller.f_code.co_name
        if caller_module == 'distutils.dist' or caller_name == 'run_commands':
            _install.run(self)
        else:
            self.do_egg_install()
        self.run_command('generate_configuration_files')

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
    'setup_requires': ['appdirs', ],
    'install_requires': required_packages,
    'include_package_data': True,
    'entry_points': {
        'console_scripts': [
            'counterblock = counterblock:server_main',
        ]
    },
    'cmdclass': {
        'install': install,
        'generate_configuration_files': generate_configuration_files
    },    
    'package_data': {
        'counterblock.schemas': ['asset.schema.json', 'feed.schema.json'],
    }
}

if os.name == "nt":
    sys.exit("Windows installs not supported")

setup(**setup_options)
