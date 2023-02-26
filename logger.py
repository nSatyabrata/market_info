import logging
from io import StringIO

log_buffer = StringIO()


def get_logger(name: str, buffer: StringIO) -> logging.Logger:

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s')

    stream_handler = logging.StreamHandler(buffer)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger

    