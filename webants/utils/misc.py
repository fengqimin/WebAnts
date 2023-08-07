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
    """返回对象的拷贝，其中kwargs中给定的值将变化
        如果kwargs中设定了cls，将实现类转换
    """
    if not hasattr(obj, '__dict__'):
        return obj

    for x in obj.__dict__:
        kwargs.setdefault(x, getattr(obj, x))

    cls: type = kwargs.pop('cls', obj.__class__)

    return cls(*args, **kwargs)
