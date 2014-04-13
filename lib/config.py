# -*- coding: utf-8 -*-
VERSION = 0.1

DB_VERSION = 21 #a db version increment will cause counterwalletd to rebuild its database off of counterpartyd 

CAUGHT_UP = False #atomic state variable, set to True when counterpartyd AND counterwalletd are caught up

UNIT = 100000000

SUBDIR_ASSET_IMAGES = "asset_img" #goes under the data dir and stores retrieved asset images

MARKET_PRICE_DERIVE_NUM_POINTS = 6 #number of last trades over which to derive the market price
MARKET_PRICE_DERIVE_WEIGHTS = [1, .9, .72, .6, .4, .3] #good first guess...maybe
assert(len(MARKET_PRICE_DERIVE_WEIGHTS) == MARKET_PRICE_DERIVE_NUM_POINTS) #sanity check
