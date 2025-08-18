import re
from typing import Any
import hashlib

try:
    import ujson as json
except ImportError:
    import json

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
        *args: Positional arguments passed to the new class constructor
        **kwargs: Keyword arguments that will override object attributes.
                 Special 'cls' parameter can be used to change the class.
    """
    if not hasattr(obj, '__dict__'):
        return obj

    # 提取目标类，默认为原对象的类
    cls: type = kwargs.pop('cls', obj.__class__)

    # 构造函数签名适配：只保留属于目标类 __init__ 的参数
    import inspect
    try:
        sig = inspect.signature(cls.__init__)
        init_params = set(sig.parameters.keys())
        init_kwargs = {k: v for k, v in kwargs.items() if k in init_params or k == 'cls'}
    except Exception:
        # 若无法获取签名，则不过滤参数
        init_kwargs = kwargs.copy()

    # 创建新实例
    try:
        new_obj = cls(*args, **init_kwargs)
    except TypeError:
        # 若构造失败，尝试无参构造再更新属性
        new_obj = cls()

    # 更新属性（包括未用于构造的参数）
    for key, value in kwargs.items():
        setattr(new_obj, key, value)

    return new_obj


def read_html(html_file, encoding=None):
    if encoding is None:
        with open(html_file, 'rb') as fp:
            data = fp.read()
    else:
        with open(html_file, 'r', encoding=encoding) as fp:
            data = fp.read()
    return data


def hash_id(string) -> str:
    assert isinstance(string, (str, bytes))
    if isinstance(string, str):
        string = string.encode()
    id_ = hashlib.sha1()
    id_.update(string)
    return id_.hexdigest()


def sha1sum(string) -> str:
    assert isinstance(string, (str, bytes))
    if isinstance(string, str):
        string = string.encode()
    sha1 = hashlib.sha1()
    sha1.update(string)
    return sha1.hexdigest()


def deduplicate_from_list(_list, key):
    """利用集合实现高效的列表去重复项，并确保列表内各元素的顺序不变
    :return: list,
    """
    key = key if callable(key) else lambda x: x
    _scanned_set = set()
    result = []
    for item in _list:
        scanned_key = key(item)
        if scanned_key in _scanned_set:
            continue
        _scanned_set.add(scanned_key)
        result.append(item)
    return result


def load_json(json_file: str):
    """从文件中载入json
    """
    if isinstance(json_file, str):
        file = json_file
        with open(file, 'r', encoding='utf-8') as fp:
            data = json.load(fp)
            return data
    else:
        return None


def dump_json(data, file):
    """导出已经被爬取url的json

    :param data:已经被请求url的列表
    :param file:json文件
    :return:
    """
    with open(file, 'w', encoding='utf-8') as fp:
        json.dump(data, fp)


def load_requested_urls(file_or_list):
    """载入已经被爬取ulr的json
    """
    if isinstance(file_or_list, str):
        file = file_or_list
        with open(file, 'r', encoding='utf-8') as fp:
            data = json.load(fp)
    else:
        data = file_or_list

    data = set(data)

    return data


def dump_requested_url(done_urls, file):
    """导出已经被爬取url的json

    :param done_urls:已经被请求url的列表
    :param file:json文件
    :return:
    """
    if isinstance(done_urls, set):
        done_urls = list(done_urls)

    if file:
        with open(file, 'w', encoding='utf-8') as fp:
            json.dump(done_urls, fp)


def json2db(json_file, db_file=r'D:\My Projects\Python\WebCrawler\copy_hkpic.db'):
    import sqlite3
    import hashlib
    status = 'SUCCESS'

    with open(json_file, 'r', encoding='utf-8') as fp:
        data = json.load(fp)
    db_conn = sqlite3.connect(db_file)
    cur = db_conn.cursor()
    # print(len(data))
    data = set(data)
    # print(len(data))

    for url in data:
        col_id = hashlib.sha1()
        col_id.update(url.encode())
        col_id.update(status.encode())
        col_url = url
        col_status = status
        column = (col_id.hexdigest(), col_url, col_status)
        print(column)
        cur.execute('INSERT INTO seen_urls VALUES (?,?,?)', column)
    cur.close()
    db_conn.commit()
    db_conn.close()
