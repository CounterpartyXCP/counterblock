## Client Versions ##
* v1.3.0 (2015-10-31)
    * Fixes periodic `blockfeed` hanging issue (where `counterblock` would still run, but not process new blocks from `counterparty-server`)
    * Block processing is much more robust now if an exception is encountered (e.g. counterparty-server goes down). Should prevent additional hanging-type issues
    * Tweaked `blockfeed` "caught up" checking logic. Should be more reliable
    * Simplified `blockchain` module -- we call API methods on `counterparty-server` now, whereever possible, instead of reimplementing them on `counterblock`
    * Enhance the information returned with `GET /_api`. Several new parameters added, including `ERROR` for easier diagnosing of most common error conditions.
    * `GET /_api` now returns CORS headers, allowing it to be used with cross domain requests
    * Added this `ChangeLog.md` file
* v1.2.0 (2015-09-15)
    * Move most counterblock functionality into plug-in modules.
    * Pegs the pymongo library version, to avoid incompatibility issues with pymongo 3.0
    * Improves exception logging for exceptions that happen within a greenlet thread context
    * Fixes an issue with an exception on reorg processing (from counterparty-server's message feed).
    * Modifies the state flow with rollbacks to streamline and simplify things a bit
* 1.1.1 (2015-03-23)
    * Fix some (uncaught) runtime errors that can cause significant stability problems in Counterblock in certain cases.
    * Properly implements logging of uncaught errors to the counterblock log files.
* 1.1.0 (2015-02-06)
    * Updated to work with the new architecture of counterparty 9.49.4 (the configuration options have been adjusted to be like counterparty, e.g. no more --data-dir)
    * new configuration file locations by default
    * Added setup.py for setuptools / pip
* 1.0.1 (2015-01-23)
    * block state variable naming change
    * get_jsonrpc_api() fix with abort_on_error=False
    * bug fix with URL fetching, abort_on_error setting
    * Fix for ambivalent variable name
    * fix division per zero
* 1.0.0 (2015-01-05)
    * MAJOR: Added plugin (modular) functionality. counterblockd can now be easily extended to support custom functionality
    * Increased JSON API request timeout to 120 seconds
    * Implemented support for new order_match id format
    * Implemented always trying/retrying for RPC calls
    * Removed Callback and RPS
    * Modularized Counterblockd functionality & plugin interface for third-party modules
    * Optimized blockfeeds.py
    * Fixed the difference of one satoshi between BTC balances returned by counterpartyd and counterblockd
    * Implemented an alternative for counterpartyd api get_asset_info() method to speed up the login in counterwallet for wallet with a lot of assets
    * Updated versions of deps (fixes issue with fetching SSL urls)
    * Fixed the issue with passing JSON data in POST requests
    * Added rollback command line argument and RollbackProcessor
