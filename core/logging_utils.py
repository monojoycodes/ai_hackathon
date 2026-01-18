import logging
import os

LOG_PATH = os.path.join("outputs", "pipeline.log")


def get_logger(name: str) -> logging.Logger:
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    logger = logging.getLogger(name)
    log_path = os.path.abspath(LOG_PATH)
    has_file = any(
        isinstance(handler, logging.FileHandler) and handler.baseFilename == log_path
        for handler in logger.handlers
    )
    has_stream = any(
        isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler)
        for handler in logger.handlers
    )
    if has_file and has_stream:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    if not has_file:
        file_handler = logging.FileHandler(LOG_PATH)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if not has_stream:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger
