import os, sys

def server_main():
    from counterblock import server
    server.main()

def armory_utxsvr_main():
    from counterblock import armory_utxsvr
    armory_utxsvr.main()
