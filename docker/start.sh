#!/bin/bash

# Run "setup.py develop" if we need to
if [ ! -d /counterblock/counterblock.egg-info ]; then
    cd /counterblock; python3 setup.py develop; cd /
fi

# Kick off the server, defaulting to the "server" subcommand
: ${PARAMS:=""}
: ${COMMAND:="server"}
/usr/local/bin/counterblock ${PARAMS} ${COMMAND}

