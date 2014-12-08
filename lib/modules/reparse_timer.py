import time
import logging
import sys
from lib import config
from lib.processor import processor

@processor.StartUpProcessor.subscribe()
def reparse_timer(): 
    config.REPARSE_FORCED = True
    config.state['timer'] = time.time()
    logging.info("Started reparse timer")

@processor.CaughtUpProcessor.subscribe(priority=90)
def reparse_timer(): 
    logging.warn("Caught Up To Blockchain, time elapsed %s" %(time.time() - config.state['timer']))
    sys.exit(1)
