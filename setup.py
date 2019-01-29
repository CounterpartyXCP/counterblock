#!/usr/bin/env python3

from setuptools import setup, find_packages, Command
from setuptools.command.install import install as _install
from setuptools.command.bdist_egg import bdist_egg as _bdist_egg
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
        from counterblock.lib import config_util
        config_util.generate_config_files()

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
    'appdirs==1.4.0',
    'prettytable==0.7.2',
    'python-dateutil==2.5.3',
    'flask==0.11.1',
    'json-rpc==1.10.3',
    'pytest==2.9.2',
    'pycoin==0.77',
    #'python-bitcoinlib==0.10.1', <-- restore this when python-bitcoinlib 0.10.x with bech32 support is released
    'pymongo==3.2.2',
    'gevent==1.1.1',
    'greenlet==0.4.9',
    'grequests==0.3.0',
    'redis==2.10.5',
    'pyzmq==15.2.0',
    'pillow==3.2.0',
    'lxml==3.6.0',
    'jsonschema==2.5.1',
    'strict_rfc3339==0.7',
    'rfc3987==1.3.6',
    'aniso8601==1.1.0',
    'geoip2==2.9.0',
    'colorama==0.3.7',
    'configobj==5.0.6',
    'repoze.lru==0.6'
]

setup_options = {
    'name': 'counterblock',
    'version': config.VERSION,
    'author': 'Counterparty Developers',
    'author_email': 'support@counterparty.io',
    'maintainer': 'Counteparty Developers',
    'maintainer_email': 'dev@counterparty.io',
    'url': 'http://counterparty.io',
    'license': 'MIT',
    'description': 'counterblock server',
    'long_description': 'Implements support for extended functionality for counterparty-lib',
    'keywords': 'counterparty, bitcoin, counterblock',
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
