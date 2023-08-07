# -*- coding: utf-8 -*-
import hashlib
import re
from typing import Sequence
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

__all__ = [
    'REGEX_BASE',
    'REGEX_HREF',
    'REGEX_HREF_A',
    'REGEX_TITLE',
    'get_url_path',
    'lenient_host',
    'normalize_url',
    'url_fingerprint',
    'url_fp_to_filename',
    'url_to_path',
    'url_with_host',

]

# <base href="http://www.w3school.com.cn/i/" />
REGEX_BASE = re.compile(r'''<base.+?href\s*=["']([^\s"'<>]+)["']''', flags=re.I)
REGEX_HREF = re.compile(r'''href\s*=["']([^\s"'<>]+)["']''', flags=re.I)
REGEX_HREF_A = re.compile(r'''<a.+?href\s*=["']([^\s"'<>]+)["']''', flags=re.I)
REGEX_TITLE = re.compile(r'<title.*?>(.+)<.*?/title>', flags=re.I)


def get_url_path(url: str, base_url: str = None) -> str:
    """
    如果url中包含base_url，则返回url中的路径，否则返回整个url
    用途主要是为了对存在多个域名的网站进行处理
    :param url:
    :param base_url:
    :return:
    """
    if base_url in url:
        return urlparse(url).path
    else:
        return url


def lenient_host(host: str) -> str:
    """将host松散化

        www.baidu.com -> baiducom
        108.170.5.99 -> 108.170.5.99
    :param host:
    :return:
    """
    if re.match(r'^[\d.]+$', host):
        return host
    parts = host.split('.')[-2:]

    return ''.join(parts)


def normalize_url(url: str,
                  keep_auth: bool = False,
                  keep_fragments: bool = False,
                  keep_blank_values: bool = True,
                  keep_default_port: bool = False,
                  sort_query: bool = True,
                  ) -> str:
    """规范化url

        默认情况下，去除认证信息， 去除片段，去除默认端口，保留查询空值，排序查询；
    :param url:
    :param keep_auth: 是否保留认证信息， 默认为False
    :param keep_fragments: 是否保留fragment，默认为False
    :param keep_blank_values: 是否保留查询空值， 默认为True
    :param keep_default_port: 是否保留默认端口， 默认为False
    :param sort_query: 是否排序查询， 默认为True
    :return:
    """
    (scheme, netloc, path, params, query, fragment) = urlparse(url)
    # 处理查询空值
    qsl = parse_qsl(query, keep_blank_values)
    # 排序查询
    if sort_query:
        query = urlencode(sorted(qsl))
    # 处理fragment
    fragment = fragment if keep_fragments else ''
    # 去除默认端口，如果keep_default_port=False
    if not keep_default_port:
        if scheme == 'http':
            netloc = netloc.removesuffix(':80')
        elif scheme == 'https':
            netloc = netloc.removesuffix(':443')

    if not keep_auth:
        netloc = netloc.rsplit('@', maxsplit=1)[-1]

    url = urlunparse((scheme, netloc, path, params, query, fragment))

    return url


def url_fingerprint_byte(url: str,
                         method: str = 'GET',
                         *,
                         algorithm_name: str = 'sha1',
                         keep_auth: bool = False,
                         keep_blank_values: bool = True,
                         keep_default_port: bool = False,
                         keep_fragments: bool = False,
                         sort_query: bool = True,
                         new_host: str = None) -> bytes:
    """url normalize and hash

        :param url:
        :param method:
        :param algorithm_name:
        :param keep_auth:
        :param sort_query:
        :param keep_blank_values:
        :param keep_fragments:
        :param keep_default_port:
        :param new_host:

    """
    if new_host:
        url = url_with_host(url, new_host)

    url = normalize_url(url,
                        keep_auth=keep_auth,
                        sort_query=sort_query,
                        keep_fragments=keep_fragments,
                        keep_blank_values=keep_blank_values,
                        keep_default_port=keep_default_port, )

    _hash = hashlib.new(algorithm_name, method.upper().encode())
    _hash.update(url.encode())

    return _hash.digest()


def url_fingerprint(url: str,
                    method: str = 'GET',
                    *,
                    algorithm_name: str = 'sha1',
                    keep_auth: bool = False,
                    keep_blank_values: bool = True,
                    keep_default_port: bool = False,
                    keep_fragments: bool = False,
                    sort_query: bool = True,
                    new_host: str = None) -> str:
    """url normalize and hash   

        :param url:
        :param method:
        :param algorithm_name:
        :param keep_auth:
        :param sort_query:
        :param keep_blank_values:
        :param keep_fragments:
        :param keep_default_port:
        :param new_host: 

    """
    if new_host:
        url = url_with_host(url, new_host)

    url = normalize_url(url,
                        keep_auth=keep_auth,
                        sort_query=sort_query,
                        keep_fragments=keep_fragments,
                        keep_blank_values=keep_blank_values,
                        keep_default_port=keep_default_port, )

    _hash = hashlib.new(algorithm_name, method.upper().encode())
    _hash.update(url.encode())

    return _hash.hexdigest()


def url_fp_to_filename(url,
                       *,
                       algorithm_name: str = 'sha1',
                       keep_auth: bool = False,
                       sort_query: bool = True,
                       keep_blank_values: bool = True,
                       keep_fragments: bool = False,
                       keep_default_port: bool = False,
                       new_host: str = None,
                       suffix: str = None,
                       ) -> str:
    """利用url的指纹命名文件名

    :param url:
    :param algorithm_name:
    :param keep_auth:
    :param sort_query:
    :param keep_blank_values:
    :param keep_fragments:
    :param keep_default_port:
    :param new_host:
    :param suffix
    :return:
    """
    _hash = url_fingerprint(url=url_with_host(url, new_host),
                            algorithm_name=algorithm_name,
                            keep_auth=keep_auth,
                            sort_query=sort_query,
                            keep_blank_values=keep_blank_values,
                            keep_fragments=keep_fragments,
                            keep_default_port=keep_default_port,
                            new_host=new_host)

    return _hash + '.' + (suffix or 'html')


def url_to_path(url: str) -> str:
    """将URL表示为路径 。

    :param url:
    :return: 路径字符串
    """
    parts = urlparse(url)
    assert parts.scheme in (
        'http', 'https', 'file'), f"Expected URL scheme ('http', 'https', 'file'), got {parts.scheme}. "

    return parts.path.split('/', maxsplit=1)[1]


def url_with_host(url: str, new_host: str = None, key: Sequence = None) -> str:
    if new_host is None:
        return url

    host_old = urlparse(url).hostname
    if key:
        if host_old in key:
            url = url.replace(host_old, new_host, 1)
    else:
        url = url.replace(host_old, new_host, 1)
    return url
