"""Request Module

This module provides a robust Request class with:
- Middleware integration
- Request lifecycle hooks
- Customizable request attributes
- Validation and error handling
- Priority queueing support
"""

import asyncio
from asyncio import iscoroutinefunction
from typing import Any, Callable, Dict, Optional, Union, List
from urllib.parse import urlparse, urlunparse

from webants.libs.exceptions import InvalidRequestMethod
from webants.utils.url import normalize_url

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
}


class Request:
    """Enhanced HTTP Request class."""

    METHODS: set[str] = {"GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"}
    count: int = 0  # Class-level counter for request tracking

    __slots__ = (
        "url",
        "method",
        "headers",
        "body",
        "cookies",
        "encoding",
        "callback",
        "errback",
        "cb_kwargs",
        "dont_filter",
        "priority",
        "retries",
        "delay",
        "timeout",
        "meta",
        "referer",
        "_middlewares",
        "_validate",
        "_normalize"
    )

    def __init__(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: Optional[Dict] = None,
        body: Optional[Union[str, bytes, Dict]] = None,
        cookies: Optional[Dict] = None,
        encoding: str = "utf-8",
        callback: Optional[Callable] = None,
        errback: Optional[Callable] = None,
        cb_kwargs: Optional[Dict] = None,
        dont_filter: bool = False,
        priority: int = 0,
        retries: int = 3,
        delay: float = 0.0,
        timeout: Optional[float] = None,
        meta: Optional[Dict] = None,
        referer: Optional["Request"] = None,
        validate: bool = True,
        normalize: bool = True,
    ):
        """Initialize a Request object.
        
        Args:
            url: Target URL
            method: HTTP method, must be one of METHODS
            headers: Request headers
            body: Request body
            cookies: Request cookies
            encoding: Request encoding
            callback: Success callback function
            errback: Error callback function
            cb_kwargs: Callback keyword arguments
            dont_filter: Whether to skip duplicate filtering
            priority: Request priority (lower = higher priority)
            retries: Number of retries allowed
            delay: Delay before sending request
            timeout: Request timeout
            meta: Additional metadata
            referer: Referring request
            validate: Whether to validate the request
            normalize: Whether to normalize the URL
        """
        if validate:
            self._validate_method(method)
            self._validate_url(url)
            
        self.url = normalize_url(url) if normalize else url
        self.method = method.upper()
        self.headers = {**DEFAULT_REQUEST_HEADERS, **(headers or {})}
        self.body = body
        self.cookies = cookies or {}
        self.encoding = encoding
        
        # Validation for callback functions
        if callback:
            assert iscoroutinefunction(callback), (
                f"callback must be a coroutine function, got {callback.__name__}"
            )
        if errback:
            assert iscoroutinefunction(errback), (
                f"errback must be a coroutine function, got {errback.__name__}"
            )
            
        self.callback = callback
        self.errback = errback
        self.cb_kwargs = cb_kwargs or {}
        
        # Request behavior configuration
        self.dont_filter = dont_filter
        self.priority = priority
        self.retries = retries
        self.delay = delay
        self.timeout = timeout
        
        # Additional metadata
        self.meta = meta or {}
        self.referer = referer
        
        # Track request count
        Request.count += 1
        
        # Initialize middleware list
        self._middlewares = []
        self._validate = validate
        self._normalize = normalize

    def _validate_method(self, method: str) -> None:
        """Validate HTTP method.
        
        Args:
            method: HTTP method to validate
            
        Raises:
            InvalidRequestMethod: If method is not supported
        """
        if method.upper() not in self.METHODS:
            raise InvalidRequestMethod(f"'{method}' method is not supported")

    def _validate_url(self, url: str) -> None:
        """Validate URL format.
        
        Args:
            url: URL to validate
            
        Raises:
            ValueError: If URL is invalid
        """
        if not isinstance(url, str):
            raise TypeError(f"url must be str, got {url.__class__.__name__}")
            
        parsed = urlparse(url)
        if not all([parsed.scheme, parsed.netloc]):
            raise ValueError(f"Invalid URL: {url}")

    def __repr__(self) -> str:
        """String representation."""
        return f"<Request [{self.method}] {self.url}>"

    def __lt__(self, other: "Request") -> bool:
        """Compare requests by priority."""
        return self.priority < other.priority

    def __eq__(self, other: object) -> bool:
        """Compare requests for equality."""
        if not isinstance(other, Request):
            return NotImplemented
        return self.fingerprint() == other.fingerprint()

    def __hash__(self) -> int:
        """Hash for request deduplication."""
        return hash(self.fingerprint())

    def fingerprint(
        self,
        *,
        keep_fragments: bool = False,
        keep_auth: bool = False,
        normalize: bool = True
    ) -> str:
        """Generate unique fingerprint for request.
        
        Args:
            keep_fragments: Whether to keep URL fragments
            keep_auth: Whether to keep authentication info
            normalize: Whether to normalize the URL
            
        Returns:
            Request fingerprint string
        """
        # Create parts list for fingerprint
        parts = [
            self.method,
            normalize_url(
                self.url,
                keep_fragments=keep_fragments,
                keep_auth=keep_auth,
            )
        ]
        
        # Add body to fingerprint if present
        if self.body:
            if isinstance(self.body, (str, bytes)):
                parts.append(str(self.body))
            else:
                # For dict/json bodies, sort keys for consistency
                parts.append(str(sorted(self.body.items())))
                
        return ":".join(parts)

    def copy(self, **kwargs: Any) -> "Request":
        """Create copy of request with optional modifications.
        
        Args:
            **kwargs: Attributes to override
            
        Returns:
            New Request instance
        """
        # Start with current attributes
        new_kwargs = {
            "url": self.url,
            "method": self.method,
            "headers": self.headers.copy(),
            "body": self.body,
            "cookies": self.cookies.copy(),
            "encoding": self.encoding,
            "callback": self.callback,
            "errback": self.errback,
            "cb_kwargs": self.cb_kwargs.copy(),
            "dont_filter": self.dont_filter,
            "priority": self.priority,
            "retries": self.retries,
            "delay": self.delay,
            "timeout": self.timeout,
            "meta": self.meta.copy(),
            "referer": self.referer,
            "validate": self._validate,
            "normalize": self._normalize
        }
        
        # Override with provided kwargs
        new_kwargs.update(kwargs)
        
        return Request(**new_kwargs)

    def replace(self, *args: Any, **kwargs: Any) -> "Request":
        """Replace request attributes in place.
        
        Args:
            **kwargs: Attributes to replace
            
        Returns:
            Self for chaining
        """
        for k, v in kwargs.items():
            if not hasattr(self, k):
                raise AttributeError(f"Request has no attribute '{k}'")
            setattr(self, k, v)
        return self
        
    async def prepare(self) -> None:
        """Prepare request before sending.
        
        This runs any registered middleware and performs final validation.
        """
        for middleware in self._middlewares:
            try:
                await middleware.process_request(self)
            except Exception as e:
                if self.errback:
                    await self.errback(e, self)
                raise

    def add_middleware(self, middleware: Any) -> None:
        """Add middleware to request processing chain.
        
        Args:
            middleware: Middleware instance to add
        """
        self._middlewares.append(middleware)
