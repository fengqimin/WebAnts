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
        """

        :param url:目标URL
        :param method:请求方式，必须在METHOD中
        :param headers:请求头
        :param referer:Request 来源地址, 是本网页的 Request
        :param callback: 回调函数
        :param cb_kwargs: 回调函数的关键字参数
        :param delay: 延时时间s
        :param timeout: 超时时间s
        :param retries: 重试次数
        :param priority: 优先级, 越小越高
        :param unique: 是否唯一，默认为True
        """

        if not isinstance(url, str):
            raise TypeError(f"url must be str objects, got {url.__class__.__name__}")

        self.url = url
        self.method: str = method.upper()

        if self.method not in self.METHOD:
            raise InvalidRequestMethod(f"{self.method} method is not supported.")

        self.headers: dict = headers or DEFAULT_REQUEST_HEADERS

        self.referer: Request = referer
        # 回调函数及参数
        self.callback = callback
        self.cb_kwargs = cb_kwargs or {}
        # requests config
        self.priority = priority
        self.retries = retries
        self.timeout = timeout
        self.delay = delay
        self.unique = unique
        Request.count += 1

    def __repr__(self):
        return f"<Request({self.method} {self.url})[{self.priority}]>"

    def __lt__(self, other):
        """“富比较”方法。
        x<y 调用 x.__lt__(y)
        x<=y 调用 x.__le__(y)
        x==y 调用 x.__eq__(y)
        x!=y 调用 x.__ne__(y)
        x>y 调用 x.__gt__(y)
        x>=y 调用 x.__ge__(y)。
        """
        assert isinstance(other, Request)
        return self.url < other.url

    def __gt__(self, other):
        assert isinstance(other, Request)
        return self.url > other.url

    def __eq__(self, other):
        assert isinstance(other, Request)
        return self.url == other.url

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
        """利用指定的hash算法，计算request的指纹（哈希值）


        :param algorithm_name: 哈希算法的名称， 默认使用sha1
        :param keep_auth: 是否保留认证信息， 默认为False，以确保不因用户的不同导致哈希值不同
        :param keep_fragments: 是否保留fragment，默认为False，以确保不因fragment的不同导致哈希值不同
        :param keep_blank_values: 是否保留查询空值， 默认为True
        :param keep_default_port: 是否保留默认端口， 默认为False
        :param sort_query: 是否排序查询， 默认为True，以确保不因查询参数的顺序不同导致哈希值不同

        :return:
        """
        import hashlib

        # 开始计算哈希值
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
