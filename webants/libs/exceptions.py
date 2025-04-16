"""Exceptions Module

This module defines a comprehensive exception hierarchy for the crawler with:
- Categorized exceptions
- Error tracking
- Retry policies
- Integration with monitoring
"""

from typing import Any, Dict, Optional, Type


class WebAntsException(Exception):
    """Base exception class for WebAnts crawler."""
    
    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        retryable: bool = False,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Initialize exception.
        
        Args:
            message: Error message
            code: Error code for categorization
            retryable: Whether error can be retried
            metadata: Additional error context
        """
        super().__init__(message)
        self.code = code or self.__class__.__name__
        self.retryable = retryable
        self.metadata = metadata or {}
        
    def __str__(self) -> str:
        """String representation with metadata."""
        parts = [super().__str__()]
        if self.code:
            parts.append(f"[{self.code}]")
        if self.metadata:
            parts.append(str(self.metadata))
        return " ".join(parts)


# Request/Response Exceptions
class RequestError(WebAntsException):
    """Base class for request-related errors."""
    pass


class InvalidRequestMethod(RequestError):
    """Invalid HTTP method specified."""
    pass


class InvalidURL(RequestError):
    """Invalid URL format."""
    pass


class RequestTimeout(RequestError):
    """Request timed out."""
    
    def __init__(
        self,
        message: str = "Request timed out",
        *,
        timeout: float,
        **kwargs
    ):
        """Initialize timeout error.
        
        Args:
            message: Error message
            timeout: Timeout duration
            **kwargs: Additional metadata
        """
        super().__init__(
            message,
            code="TIMEOUT",
            retryable=True,
            metadata={"timeout": timeout, **kwargs}
        )


class TooManyRedirects(RequestError):
    """Too many redirects encountered."""
    
    def __init__(
        self,
        message: str = "Too many redirects",
        *,
        redirect_count: int,
        **kwargs
    ):
        """Initialize redirect error.
        
        Args:
            message: Error message
            redirect_count: Number of redirects
            **kwargs: Additional metadata
        """
        super().__init__(
            message,
            code="REDIRECT_LIMIT",
            retryable=False,
            metadata={"redirect_count": redirect_count, **kwargs}
        )


class ConnectionError(RequestError):
    """Network connection error."""
    
    def __init__(
        self,
        message: str,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        **kwargs
    ):
        """Initialize connection error.
        
        Args:
            message: Error message
            host: Target host
            port: Target port
            **kwargs: Additional metadata
        """
        super().__init__(
            message,
            code="CONNECTION_ERROR",
            retryable=True,
            metadata={
                "host": host,
                "port": port,
                **kwargs
            }
        )


# Parser Exceptions
class ParserError(WebAntsException):
    """Base class for parsing errors."""
    pass


class InvalidSelector(ParserError):
    """Invalid selector syntax."""
    pass


class ExtractorError(ParserError):
    """Data extraction error."""
    
    def __init__(
        self,
        message: str,
        *,
        selector: Optional[str] = None,
        field: Optional[str] = None,
        **kwargs
    ):
        """Initialize extractor error.
        
        Args:
            message: Error message
            selector: Failed selector
            field: Target field
            **kwargs: Additional metadata
        """
        super().__init__(
            message,
            code="EXTRACTION_ERROR",
            retryable=False,
            metadata={
                "selector": selector,
                "field": field,
                **kwargs
            }
        )


# Spider Exceptions
class SpiderError(WebAntsException):
    """Base class for spider errors."""
    pass


class SpiderConfigError(SpiderError):
    """Spider configuration error."""
    pass


class SpiderInitError(SpiderError):
    """Spider initialization error."""
    pass


class SpiderStopError(SpiderError):
    """Spider stopped due to error."""
    pass


# Middleware Exceptions
class MiddlewareError(WebAntsException):
    """Base class for middleware errors."""
    pass


class CircuitBreakerError(MiddlewareError):
    """Circuit breaker is open."""
    
    def __init__(
        self,
        message: str = "Circuit breaker is open",
        *,
        domain: str,
        failures: int,
        **kwargs
    ):
        """Initialize circuit breaker error.
        
        Args:
            message: Error message
            domain: Affected domain
            failures: Number of failures
            **kwargs: Additional metadata
        """
        super().__init__(
            message,
            code="CIRCUIT_OPEN",
            retryable=False,
            metadata={
                "domain": domain,
                "failures": failures,
                **kwargs
            }
        )


# Storage Exceptions
class StorageError(WebAntsException):
    """Base class for storage errors."""
    pass


class PersistenceError(StorageError):
    """Data persistence error."""
    pass


class LoadError(StorageError):
    """Data loading error."""
    pass


# Rate Limiting
class RateLimitError(WebAntsException):
    """Rate limit exceeded."""
    
    def __init__(
        self,
        message: str = "Rate limit exceeded",
        *,
        domain: str,
        limit: Optional[int] = None,
        reset_after: Optional[float] = None,
        **kwargs
    ):
        """Initialize rate limit error.
        
        Args:
            message: Error message
            domain: Rate limited domain
            limit: Rate limit value
            reset_after: Seconds until reset
            **kwargs: Additional metadata
        """
        super().__init__(
            message,
            code="RATE_LIMIT",
            retryable=True,
            metadata={
                "domain": domain,
                "limit": limit,
                "reset_after": reset_after,
                **kwargs
            }
        )


# Authentication
class AuthenticationError(WebAntsException):
    """Authentication failed."""
    
    def __init__(
        self,
        message: str = "Authentication failed",
        *,
        domain: str,
        status_code: Optional[int] = None,
        **kwargs
    ):
        """Initialize authentication error.
        
        Args:
            message: Error message
            domain: Authentication domain
            status_code: HTTP status code
            **kwargs: Additional metadata
        """
        super().__init__(
            message,
            code="AUTH_ERROR",
            retryable=False,
            metadata={
                "domain": domain,
                "status_code": status_code,
                **kwargs
            }
        )


class ExceptionTracker:
    """Track and analyze exceptions."""
    
    def __init__(self):
        """Initialize exception tracker."""
        self.exceptions: Dict[str, Dict[str, Any]] = {}
        
    def track(
        self,
        exception: WebAntsException,
        handled: bool = True
    ) -> None:
        """Track an exception occurrence.
        
        Args:
            exception: Exception instance
            handled: Whether exception was handled
        """
        code = exception.code
        if code not in self.exceptions:
            self.exceptions[code] = {
                "count": 0,
                "handled": 0,
                "unhandled": 0,
                "first_seen": None,
                "last_seen": None,
                "metadata": {}
            }
            
        stats = self.exceptions[code]
        stats["count"] += 1
        if handled:
            stats["handled"] += 1
        else:
            stats["unhandled"] += 1
            
        # Track timing
        import time
        now = time.time()
        if not stats["first_seen"]:
            stats["first_seen"] = now
        stats["last_seen"] = now
        
        # Merge metadata
        stats["metadata"].update(exception.metadata)
        
    def get_stats(self) -> Dict[str, Any]:
        """Get exception statistics.
        
        Returns:
            Dictionary with exception stats
        """
        return {
            code: {
                **stats,
                "rate": (
                    stats["count"]
                    / (stats["last_seen"] - stats["first_seen"])
                    if stats["first_seen"]
                    else 0
                )
            }
            for code, stats in self.exceptions.items()
        }
        
    def clear(self) -> None:
        """Clear tracked exceptions."""
        self.exceptions.clear()


# Global exception tracker
exception_tracker = ExceptionTracker()
