# -*- coding: utf-8 -*-
import logging
from typing import Optional

__all__ = [
    'get_logger',
]


def get_logger(log_name: str,
               log_file: Optional[str] = None,
               log_level: int = logging.INFO,
               file_handler_level: int = logging.INFO,
               stream_handler_level: int = logging.DEBUG) -> logging.Logger:
    """Configure and return a customized logger instance.
    
    Creates a logger with both file and stream handlers, allowing different log levels
    for each output destination. The logger name will be converted to uppercase.
    
    Args:
        log_name: Name identifier for the logger (will be converted to uppercase)
        log_file: Path to log file (optional, file logging disabled if None)
        log_level: Overall minimum logging level for the logger
        file_handler_level: Minimum logging level for file output
        stream_handler_level: Minimum logging level for console output
        
    Returns:
        Configured logger instance with requested handlers and levels
    """
    # Initialize logger with uppercase name
    _logger = logging.getLogger(log_name.upper())
    
    # Prevent duplicate handlers if logger already exists
    if _logger.handlers:
        return _logger

    # Configure file handler if log file path is provided
    if log_file:
        try:
            # Create file handler with UTF-8 encoding
            file_handler = logging.FileHandler(filename=log_file, encoding='utf-8')
            file_handler.setLevel(file_handler_level)
            
            # File log format: [line no] - timestamp - message
            file_formatter = logging.Formatter(
                '%(lineno)4d - %(asctime)s - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            _logger.addHandler(file_handler)
        except (IOError, PermissionError) as e:
            print(f"Failed to create file handler: {e}")

    # Configure console (stream) handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(stream_handler_level)
    
    # Console log format: timestamp:[logger]level:message
    stream_formatter = logging.Formatter(
        '%(asctime)s:[%(name)s]%(levelname)s:%(message)s'
    )
    stream_handler.setFormatter(stream_formatter)
    _logger.addHandler(stream_handler)
    
    # Set overall minimum logging level
    _logger.setLevel(log_level)
    
    # Prevent propagation to root logger
    _logger.propagate = False
    
    return _logger
