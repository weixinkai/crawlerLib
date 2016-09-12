import logging

DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

def generate_logger(name, loggerPath, console_level, file_level):
    formatter = logging.Formatter('%(asctime)s - %(name)s:(%(levelname)s) %(message)s')

    file_log = logging.FileHandler(loggerPath)
    file_log.setLevel(file_level)
    file_log.setFormatter(formatter)

    console_log = logging.StreamHandler()
    console_log.setLevel(console_level)
    console_log.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_log)
    logger.addHandler(console_log)
    return logger
