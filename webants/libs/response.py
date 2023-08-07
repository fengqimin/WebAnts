"""

    Response      = Status-Line               ; Section 6.1
                   *(( general-header        ; Section 4.5
                    | response-header        ; Section 6.2
                    | entity-header ) CRLF)  ; Section 7.1
                   CRLF
                   [ message-body ]          ; Section 7.2
    Status-Line = HTTP-Version SP Status-Code SP Reason-Phrase CRLF

    Status-Code:
    https://baike.baidu.com/item/HTTP/243074
    1xx:信息
    2xx:成功
    3xx:重定向
    4xx:客户端错误
    5xx:服务器错误
    6xx:异常（自定义）
    600:

"""

import weakref
from email.message import Message
from http.cookies import SimpleCookie
from typing import Any, Optional, Tuple, Mapping
from urllib.parse import urldefrag, urlparse

import aiohttp
from multidict import CIMultiDictProxy


try:
    import charset_normalizer as chardet
except ImportError:
    import cchardet as chardet

from webants.libs.request import Request
from webants.libs import get_html_title

UNKNOWN = None


def get_encoding(headers: Mapping = None, content: bytes = None):
    """Get encoding from request headers or content."""
    if not headers and not content:
        raise ValueError("headers 和 content 不能同时为 None")

    encoding = UNKNOWN

    content_type = headers.get("content-type") if headers else None
    if content_type:
        m = Message()
        m["content-type"] = content_type
        encoding = m.get_content_charset(UNKNOWN)

    if not encoding and content:
        encoding = chardet.detect(content).get("encoding")

    return encoding


def get_mediatype(headers: Mapping):
    """Get encoding from request headers or content."""
    mediatype = UNKNOWN
    if headers is not None and isinstance(headers, Mapping):
        content_type = headers.get("content-type")
        if content_type:
            m = Message()
            m["content-type"] = content_type
            mediatype = m.get_content_type()

    return mediatype


class Response:
    """Response class"""

    __slots__ = (
        "method",
        "real_url",
        "url",
        "http_version",
        "status_code",
        "reason",
        "headers",
        "cookies",
        "body",
        "_encoding",
        "history",
        "request",
        "_cache",
    )
    count: int = 0

    def __init__(
        self,
        method: str,
        url: str,
        *,
        status_code: int,  # type: ignore[assignment] # Status-Code
        reason: str,  # Reason-Phrase
        request: Request | None,
        http_version: Optional[aiohttp.HttpVersion] = UNKNOWN,  # HTTP-Version
        headers: Optional["CIMultiDictProxy[str]"] = UNKNOWN,  # type: ignore[assignment]
        cookies: Optional[SimpleCookie[str]] = UNKNOWN,
        body: bytes = UNKNOWN,
        encoding: Optional[str] = UNKNOWN,
        history: Optional[Tuple[aiohttp.ClientResponse, ...]] = UNKNOWN,
    ) -> None:
        # from the Status-Line of the response

        self.method = method.upper()
        self.real_url = url
        self.url = urldefrag(self.real_url).url if self.real_url else UNKNOWN

        self.http_version = http_version
        self.status_code = status_code
        self.reason = reason

        self.headers = headers
        self.cookies = cookies

        self.body = body
        self._encoding = encoding

        self.history = weakref.WeakSet(history)

        self.request = request

        self._cache: dict = {}
        Response.count += 1

    def __bool__(self):
        """Returns true if `status_code` is 200 and no error"""
        return self.ok

    def __repr__(self) -> str:
        return f"<Response {self.url} [{self.status_code}]>"

    @classmethod
    async def build(cls, r: aiohttp.ClientResponse, request: Request):
        assert isinstance(
            r, aiohttp.ClientResponse
        ), f"Expected {aiohttp.ClientResponse}, got {r.__class__.__name__}"

        return cls(
            r.method,
            str(r.real_url),
            status_code=r.status,
            reason=r.reason,
            request=request,
            http_version=r.version,
            headers=r.headers,
            cookies=r.cookies,
            body=await r.read(),
            encoding=r.get_encoding(),
            history=r.history,
        )

    @property
    def encoding(self):
        return self._encoding or get_encoding(self.headers, self.body)

    @property
    def host(self):
        return urlparse(self.url).netloc

    @property
    def json(self) -> Any:
        """Read and decodes JSON response."""
        try:
            import ujson as json
        except ImportError:
            import json

        stripped = self.body.strip()  # type: ignore[union-attr]
        if not stripped:
            return None

        try:
            return json.loads(stripped.decode(self.encoding))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    @property
    def mediatype(self):
        """获取响应的MediaType媒体类型

        https://www.iana.org/assignments/media-types/media-types.xhtml
        :return:
        """
        return get_mediatype(self.headers)

    @property
    def ok(self) -> bool:
        """Returns ``True`` if ``status`` is less than ``400``, ``False`` if not."""
        return 400 > self.status_code

    @property
    def text(self) -> str | None:
        """Read response payload and decode."""
        if self.encoding is UNKNOWN:
            return None

        try:
            return self.body.decode(encoding=self.encoding)
        except UnicodeDecodeError:
            return None
        except TypeError:
            return None

    @property
    def title(self) -> str | None:
        return get_html_title(self.text)
