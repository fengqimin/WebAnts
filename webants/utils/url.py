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
    """Get the path part from the URL
    If the URL contains base_url, return the path part of the URL, otherwise return the entire URL.
    This is mainly used to process websites with multiple domains.
    
    Args:
        url: URL to process
        base_url: Base URL for comparison
    
    Returns:
        Path part of URL or full URL
    """
    if base_url in url:
        return urlparse(url).path
    else:
        return url


def lenient_host(host: str) -> str:
    """Normalize host name for lenient comparison
        
    Examples:
        www.baidu.com -> baiducom
        108.170.5.99 -> 108.170.5.99
        
    Args:
        host: Host name to normalize
        
    Returns:
        Normalized host name
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
    """Normalize URL by applying standard rules
    
    By default:
    - Removes authentication info
    - Removes fragments 
    - Removes default ports
    - Preserves empty query values
    - Sorts query parameters
    
    Args:
        url: URL to normalize
        keep_auth: Whether to keep authentication info (default False)
        keep_fragments: Whether to keep URL fragments (default False)
        keep_blank_values: Whether to keep empty query values (default True)
        keep_default_port: Whether to keep default ports (default False)
        sort_query: Whether to sort query parameters (default True)
        
    Returns:
        Normalized URL
    """
    (scheme, netloc, path, params, query, fragment) = urlparse(url)
    # Handle empty query values
    qsl = parse_qsl(query, keep_blank_values)
    # Sort query params
    if sort_query:
        query = urlencode(sorted(qsl))
    # Handle fragments
    fragment = fragment if keep_fragments else ''
    # Remove default ports if keep_default_port=False
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
    """URL normalize and hash

    Args:
        url: URL to normalize and hash
        method: HTTP method to use
        algorithm_name: Hash algorithm to use
        keep_auth: Whether to keep auth info 
        keep_blank_values: Whether to keep blank query values
        keep_fragments: Whether to keep URL fragments
        keep_default_port: Whether to keep default ports
        sort_query: Whether to sort query parameters
        new_host: Optional new host to replace in URL

    Returns:
        Bytes containing URL hash
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
    """URL normalize and hash
    
    Args: 
        url: URL to normalize and hash
        method: HTTP method to use
        algorithm_name: Hash algorithm to use
        keep_auth: Whether to keep auth info
        sort_query: Whether to sort query params
        keep_blank_values: Whether to keep blank values
        keep_fragments: Whether to keep fragments
        keep_default_port: Whether to keep default ports
        new_host: Optional new host to replace

    Returns:
        String containing URL hash
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
    """Generate filename using URL fingerprint

    Args:
        url: URL to process
        algorithm_name: Hash algorithm name (default 'sha1')
        keep_auth: Whether to keep authentication info (default False)
        sort_query: Whether to sort query parameters (default True)
        keep_blank_values: Whether to keep empty query values (default True)
        keep_fragments: Whether to keep URL fragments (default False)
        keep_default_port: Whether to keep default ports (default False)
        new_host: New host to replace in URL
        suffix: File suffix (default 'html')
    
    Returns:
        Filename generated from URL fingerprint
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
    """Convert URL to path string

    Args:
        url: URL to convert
    
    Returns:
        Path string
    """
    parts = urlparse(url)
    assert parts.scheme in (
        'http', 'https', 'file'), f"Expected URL scheme ('http', 'https', 'file'), got {parts.scheme}. "

    return parts.path.split('/', maxsplit=1)[1]


def url_with_host(url: str, new_host: str = None, key: Sequence = None) -> str:
    """Replace host in URL with new host

    Args:
        url: Original URL
        new_host: New host to replace
        key: Sequence of keys for conditional replacement
    
    Returns:
        URL with replaced host
    """
    if new_host is None:
        return url

    host_old = urlparse(url).hostname
    if key:
        if host_old in key:
            url = url.replace(host_old, new_host, 1)
    else:
        url = url.replace(host_old, new_host, 1)
    return url
