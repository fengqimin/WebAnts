"""Exception Module

This module defines a series of custom exceptions used for handling various error conditions in the web crawler framework.
"""

# Define all names that can be imported from outside the module
__all__ = [
    'InvalidDownloader',
    'InvalidExtractor',
    'InvalidParser',
    "InvalidScheduler",
    'InvalidRequestMethod',
    'InvalidURL',
    'NotAbsoluteURLError',
]

class InvalidDownloader(Exception):
    """
    Raised when an invalid downloader is used.
    """
    pass

class InvalidExtractor(Exception):
    """
    Raised when an invalid extractor is used.
    """
    pass

class InvalidParser(Exception):
    """
    Raised when an invalid parser is used.
    """
    pass

class InvalidRequestMethod(Exception):
    """
    Raised when an invalid HTTP request method is used.
    """
    pass

class InvalidScheduler(Exception):
    """
    Raised when an invalid scheduler is used.
    """
    pass

class InvalidURL(Exception):
    """
    Raised when an invalid URL is used.
    """
    pass

class NotAbsoluteURLError(InvalidURL):
    """
    Raised when a URL is not absolute.
    """
    pass
