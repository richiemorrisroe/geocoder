import logging
def create_logger():
    logger = logging.getLogger("root")
    logger.setLevel(logging.DEBUG)
    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    return logger

def get_api_key(path) -> str:
    key_file = open(path, "r")
    key = key_file.readline().strip()
    return key
