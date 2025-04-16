# -*- coding: utf-8 -*-
import logging

__all__ = [
    'get_logger',
]


def get_logger(log_name: str,
               log_file=None,
               log_level=logging.INFO,
               file_handler_level=logging.INFO,
               stream_handler_level=logging.DEBUG):
    """Configure logger format

    Args:
        log_level: Overall logging level
        log_name: Name of the logger
        log_file: Log file path
        file_handler_level: Logging level for file handler
        stream_handler_level: Logging level for stream handler

    Returns:
        Configured logger object
    """
    _logger = logging.getLogger(log_name.upper())
    # logging.basicConfig()

    if log_file:
        # Create file handler
        file_handler = logging.FileHandler(filename=log_file, encoding='utf-8')
        # Will be replaced with
        file_handler.setLevel(file_handler_level)

        # Define output format
        file_formatter = logging.Formatter('%(lineno)4d - %(asctime)s - %(message)s')
        file_handler.setFormatter(file_formatter)

        # Add created file and stream handlers to logger
        _logger.addHandler(file_handler)

    # Create stream handler for output
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(stream_handler_level)

    # Define output format
    stream_formatter = logging.Formatter('%(asctime)s:[%(name)s]%(levelname)s:%(message)s')
    stream_handler.setFormatter(stream_formatter)

    # Add created stream handler to logger
    _logger.addHandler(stream_handler)

    _logger.setLevel(log_level)
    return _logger
