
VERSION = 0.1

DB_VERSION = 1 #a db version increment will cause counterwalletd to rebuild its database off of counterpartyd 

CAUGHT_UP = False #atomic state variable, set to True when counterpartyd AND counterwalletd are caught up

UNIT = 100000000
