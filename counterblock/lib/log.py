import os
import logging

from counterblock.lib import config


def set_up(verbose):
    global MAX_LOG_SIZE
    MAX_LOG_SIZE = config.LOG_SIZE_KB * 1024  # max log size of 20 MB before rotation (make configurable later)
    global MAX_LOG_COUNT
    MAX_LOG_COUNT = config.LOG_NUM_FILES

    # Initialize logging (to file and console)
    logger = logging.getLogger()  # get root logger
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Color logging on console for warnings and errors
    logging.addLevelName(logging.WARNING, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
    logging.addLevelName(logging.ERROR, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.ERROR))

    # Console logging
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter('%(levelname)s:%(module)s: %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File logging (rotated)
    if config.LOG:
        fileh = logging.handlers.RotatingFileHandler(config.LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
        fileh.setLevel(logging.DEBUG if verbose else logging.INFO)
        formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(message)s', '%Y-%m-%d-T%H:%M:%S%z')
        fileh.setFormatter(formatter)
        logger.addHandler(fileh)
        logger.info("Logging to '{}'".format(config.LOG))

    # requests/urllib3 logging (make it not so chatty)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

    # Transaction log
    if config.TX_LOG:
        tx_logger = logging.getLogger("transaction_log")  # get transaction logger
        tx_logger.setLevel(logging.DEBUG if verbose else logging.INFO)
        tx_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(message)s', '%Y-%m-%d-T%H:%M:%S%z')
        tx_handler = logging.handlers.RotatingFileHandler(config.TX_LOG, maxBytes=MAX_LOG_SIZE, backupCount=MAX_LOG_COUNT)
        tx_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
        tx_handler.setFormatter(tx_formatter)
        tx_logger.addHandler(tx_handler)
        tx_logger.propagate = False
        logger.info("Logging txes to '{}'".format(config.TX_LOG))
