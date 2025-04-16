import re
from typing import Any

__all__ = [
    'args_to_list',
    'copy_object',
    'valid_path',
]

InvalidPathRegex = re.compile(r'[\\/:*?"<>|\t\r\n]')


def valid_path(path):
    return InvalidPathRegex.sub('-', str(path))


def args_to_list(args: Any) -> list:
    if not isinstance(args, (list, tuple)):
        if args is None:
            return []
        else:
            return [args]
    return list(args)


def copy_object(obj: object, *args, **kwargs):
    """Return a copy of the object with values changed according to kwargs.
    If 'cls' is specified in kwargs, it will implement class conversion.

    Args:
        obj: Object to copy
        *args: Positional arguments
        **kwargs: Keyword arguments that will override object attributes. 
                 Special 'cls' parameter can be used to change the class.
    """
    if not hasattr(obj, '__dict__'):
        return obj

    for x in obj.__dict__:
        kwargs.setdefault(x, getattr(obj, x))

    cls: type = kwargs.pop('cls', obj.__class__)

    return cls(*args, **kwargs)
