import os
import logging

from counterblock.lib import config

MAX_LOG_SIZE = 20 * 1024 * 1024 #max log size of 20 MB before rotation (make configurable later)
MAX_LOG_COUNT = 5

def set_up(verbose):
    # Initialize logging (to file and console)
    logger = logging.getLogger() #get root logger
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    
    #Color logging on console for warnings and errors
    logging.addLevelName(logging.WARNING, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
    logging.addLevelName(logging.ERROR, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
    
    #Console logging
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter('%(levelname)s:%(module)s: %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)
    
    #File logging (rotated)
    fileh = logging.handlers.RotatingFileHandler(config.LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
    fileh.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(message)s', '%Y-%m-%d-T%H:%M:%S%z')
    fileh.setFormatter(formatter)
    logger.addHandler(fileh)
    
    #socketio logging (don't show on console in normal operation)
    socketio_log = logging.getLogger('socketio')
    socketio_log.setLevel(logging.DEBUG if verbose else logging.WARNING)
    socketio_log.propagate = False
    
    #Transaction log
    tx_logger = logging.getLogger("transaction_log") #get transaction logger
    tx_logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    tx_fileh = logging.handlers.RotatingFileHandler(config.TX_LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
    tx_fileh.setLevel(logging.DEBUG if verbose else logging.INFO)
    tx_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(message)s', '%Y-%m-%d-T%H:%M:%S%z')
    tx_fileh.setFormatter(tx_formatter)
    tx_logger.addHandler(tx_fileh)
    tx_logger.propagate = False
