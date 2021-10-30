import logging
from functools import lru_cache

@lru_cache(maxsize=1024)
def setup_logger(logger_name, log_file=None, level=logging.INFO, mode="w", only_raw=False, allow_print=False):
    lz = logging.getLogger(logger_name)
    if not only_raw:
        formatter = logging.Formatter('%(asctime)s : %(message)s')
    else:
        formatter = logging.Formatter('%(message)s')
    if log_file is not None:
        fileHandler = logging.FileHandler(log_file, mode=mode)
        fileHandler.setFormatter(formatter)
        lz.addHandler(fileHandler)
    lz.setLevel(level)
    if allow_print:
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        lz.addHandler(streamHandler)
    return lz