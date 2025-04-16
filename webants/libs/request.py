# -*- coding: utf-8 -*-
"""Request
   A request message from a client to a server includes, within the
   first line of that message, the method to be applied to the resource,
   the identifier of the resource, and the protocol version in use.

       Request       = Request-Line              ; Section 5.1
                       *(( general-header        ; Section 4.5
                        | request-header         ; Section 5.3
                        | entity-header ) CRLF)  ; Section 7.1
                       CRLF
                       [ message-body ]          ; Section 4.3

       Request-Line   = Method SP Request-URI SP HTTP-Version CRLF

       request-header = Accept                   ; Section 14.1
                      | Accept-Charset           ; Section 14.2
                      | Accept-Encoding          ; Section 14.3
                      | Accept-Language          ; Section 14.4
                      | Authorization            ; Section 14.8
                      | Expect                   ; Section 14.20
                      | From                     ; Section 14.22
                      | Host                     ; Section 14.23
                      | If-Match                 ; Section 14.24
                      | If-Modified-Since        ; Section 14.25
                      | If-None-Match            ; Section 14.26
                      | If-Range                 ; Section 14.27
                      | If-Unmodified-Since      ; Section 14.28
                      | Max-Forwards             ; Section 14.31
                      | Proxy-Authorization      ; Section 14.34
                      | Range                    ; Section 14.35
                      | Referer                  ; Section 14.36
                      | TE                       ; Section 14.39
                      | User-Agent               ; Section 14.43

 """

from asyncio import iscoroutinefunction
from typing import Callable, Final

from webants.libs.exceptions import InvalidRequestMethod
from webants.utils.url import normalize_url

DEFAULT_REQUEST_HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
}


class Request:
    """HTTP Request class"""

    METHOD: Final[set] = {"GET", "HEAD", "POST"}

    __slots__ = (
        "url",
        "method",
        "headers",
        "referer",
        "callback",
        "cb_kwargs",
        "delay",
        "timeout",
        "retries",
        "priority",
        "unique",
    )
    count: int = 0

    def __init__(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: dict = None,
        referer: "Request" = None,
        callback: Callable = None,
        cb_kwargs: dict = None,
        delay: float = 0.00,
        timeout: float = 20.0,
        retries: int = 3,
        priority: int = 0,
        unique: bool = True,
    ):
        """Initialize a Request object
        
        Args:
            url: Target URL
            method: Request method, must be one of METHOD
            headers: Request headers
            referer: Request source URL, refers to the Request of the current page
            callback: Callback function
            cb_kwargs: Callback function keyword arguments 
            delay: Delay time in seconds
            timeout: Request timeout in seconds
            retries: Number of retries
            priority: Priority, lower number means higher priority
            unique: Whether the request should be unique, defaults to True
        """

        if not isinstance(url, str):
            raise TypeError(f"url must be str objects, got {url.__class__.__name__}")

        self.url = url
        self.method: str = method.upper()

        if self.method not in self.METHOD:
            raise InvalidRequestMethod(f"'{self.method}' method is not supported.")

        self.headers: dict = headers or DEFAULT_REQUEST_HEADERS

        self.referer: Request = referer
        # Callback function and arguments
        self.callback = callback
        if self.callback:
            assert iscoroutinefunction(callback), (
                f"callback must be a coroutine function, got {callback.__name__}"
            )     
        self.cb_kwargs = cb_kwargs or {}
        # Request configuration
        self.priority = priority
        self.retries = retries
        self.timeout = timeout
        self.delay = delay
        self.unique = unique
        Request.count += 1

    def __repr__(self):
        return f"<Request('{self.method}', '{self.url}')[{self.priority}]>"

    def __lt__(self, other):
        """Rich comparison method.
        x<y calls x.__lt__(y)
        x<=y calls x.__le__(y)
        x==y calls x.__eq__(y)
        x!=y calls x.__ne__(y)
        x>y calls x.__gt__(y)
        x>=y calls x.__ge__(y).
        """
        assert isinstance(other, Request)
        return self.url < other.url

    def __gt__(self, other):
        assert isinstance(other, Request)
        return self.url > other.url

    def __eq__(self, other):
        assert isinstance(other, Request)
        return self.fingerprint() == other.fingerprint()

    def __hash__(self):
        # return self.fingerprint()
        return hash(id(self))

    def fingerprint(
        self,
        *,
        algorithm_name: str = "sha1",
        keep_auth: bool = False,
        keep_blank_values: bool = True,
        keep_default_port: bool = False,
        keep_fragments: bool = False,
        sort_query: bool = True,
    ) -> bytes:
        """Calculate the fingerprint (hash value) of the request using the specified hash algorithm


        Args:
            algorithm_name: Name of the hash algorithm, defaults to sha1
            keep_auth: Whether to retain authentication information, defaults to False to ensure hash consistency across users
            keep_fragments: Whether to retain fragments, defaults to False to ensure hash consistency across fragments
            keep_blank_values: Whether to retain blank query values, defaults to True
            keep_default_port: Whether to retain the default port, defaults to False
            sort_query: Whether to sort query parameters, defaults to True to ensure hash consistency across parameter order

        Returns:
            The fingerprint (hash value) of the request
        """
        import hashlib

        # Start calculating the hash value
        fp = hashlib.new(algorithm_name, self.method.encode())

        url = normalize_url(
            url=self.url,
            keep_auth=keep_auth,
            keep_blank_values=keep_blank_values,
            keep_default_port=keep_default_port,
            keep_fragments=keep_fragments,
            sort_query=sort_query,
        )
        fp.update(url.encode())

        return fp.digest()
