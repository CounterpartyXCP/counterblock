#!/bin/bash

# Specify defaults (defaults are overridden if defined in the environment)
DEFAULT_BACKEND_PORT=8332
DEFAULT_COUNTERPARTY_PORT=4000
DEFAULT_RPC_PORT=4100
DEFAULT_MONGODB_DATABASE=counterblockd
DEFAULT_REDIS_DATABASE=0
EXTRA_PARAMS=""
if [ -n "$TESTNET" ]; then
    EXTRA_PARAMS="${EXTRA_PARAMS} --testnet"
    DEFAULT_BACKEND_PORT=18332
    DEFAULT_COUNTERPARTY_PORT=4001
    DEFAULT_RPC_PORT=4101
    DEFAULT_MONGODB_DATABASE=counterblockd_testnet
    DEFAULT_REDIS_DATABASE=1
fi
if [ -n "$REPARSE" ]; then
    EXTRA_PARAMS="${EXTRA_PARAMS} --reparse"
fi
if [ -n "$VERBOSE" ]; then
    EXTRA_PARAMS="${EXTRA_PARAMS} --verbose"
fi

: ${BACKEND_CONNECT:="bitcoin"}
: ${BACKEND_PORT:=$DEFAULT_BACKEND_PORT}
: ${BACKEND_USER:="rpc"}
: ${BACKEND_PASSWORD:="rpc"}
: ${COUNTERPARTY_CONNECT:="counterparty"}
: ${COUNTERPARTY_PORT:=$DEFAULT_COUNTERPARTY_PORT}
: ${COUNTERPARTY_USER:="rpc"}
: ${COUNTERPARTY_PASSWORD:="rpc"}
: ${MONGODB_CONNECT:="mongodb"}
: ${MONGODB_PORT:="27017"}
: ${MONGODB_DATABASE:=$DEFAULT_MONGODB_DATABASE}
: ${MONGODB_USER:="root"}
: ${MONGODB_PASSWORD:="root"}
: ${REDIS_CONNECT:="redis"}
: ${REDIS_PORT:="6379"}
: ${REDIS_DATABASE:=$DEFAULT_REDIS_DATABASE}
: ${RPC_PORT:=$DEFAULT_RPC_PORT}
: ${COMMAND:="server"}

# Kick off the server, defaulting to the "server" subcommand
/usr/local/bin/counterblock \
  --backend-connect=${BACKEND_CONNECT} --backend-port=${BACKEND_PORT} \
  --backend-user=${BACKEND_USER} --backend-password=${BACKEND_PASSWORD} \
  --counterparty-connect=${COUNTERPARTY_CONNECT} --counterparty-port=${COUNTERPARTY_PORT} \
  --counterparty-user=${COUNTERPARTY_USER} --counterparty-password=${COUNTERPARTY_PASSWORD} \
  --mongodb-connect=${MONGODB_CONNECT} --mongodb-port=${MONGODB_PORT} --mongodb-database=${MONGODB_DATABASE} \
  --mongodb-user=${MONGODB_USER} --mongodb-password=${MONGODB_PASSWORD} \
  --redis-connect=${REDIS_CONNECT} --redis-port=${REDIS_PORT} --redis-database=${REDIS_DATABASE} \
  --rpc-port=${RPC_PORT} \
  ${EXTRA_PARAMS} ${COMMAND}
