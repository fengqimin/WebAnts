"""Exception class

"""

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
    pass


class InvalidExtractor(Exception):
    pass


class InvalidParser(Exception):
    pass


class InvalidRequestMethod(Exception):
    pass


class InvalidScheduler(Exception):
    pass


class InvalidURL(Exception):
    pass


class NotAbsoluteURLError(InvalidURL):
    pass
