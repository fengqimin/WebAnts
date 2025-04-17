"""Parser Module
This module provides HTML/XML parsing functionality with the following features:
- Efficient parsing using lxml
- CSS selector and XPath support
- Advanced caching mechanism using LRU cache
- Robust error handling with recovery
- Smart encoding detection
"""

import functools
import hashlib
import logging
from typing import Any, Callable, Optional, Union, Dict

from lxml import etree
from lxml.html import HTMLParser, fromstring
from lxml.cssselect import CSSSelector

from webants.libs.exceptions import ParserError
from webants.utils.logger import get_logger

# Size of LRU cache for parsed documents
lru_cache_limit: int = 1000


class Parser:
    """HTML/XML Parser with advanced caching and error handling."""

    def __init__(
        self,
        encoding: str = "utf-8",
        log_level: int = logging.INFO,
        max_document_size: int = 10 * 1024 * 1024,  # 10MB
    ):
        """Initialize the parser.

        Args:
            encoding: Default encoding for parsing
            log_level: Logging level
            max_document_size: Maximum document size to parse in bytes
        """
        self.encoding = encoding
        self.logger = get_logger(self.__class__.__name__, log_level=log_level)

        self._parser = HTMLParser(
            encoding=encoding,
            remove_blank_text=True,
            remove_comments=True,
            remove_pis=True,
        )
        self.max_document_size = max_document_size
        self._css_cache: Dict[str, Any] = {}
        self._xpath_cache: Dict[str, Any] = {}

        # Statistics
        self.stats = {
            "total_parses": 0,
            "cache_hits": 0,
            "parse_errors": 0,
            "encoding_errors": 0,
            "size_errors": 0,
            "css_cache_hits": 0,
            "xpath_cache_hits": 0,
            "total_css_queries": 0,
            "total_xpath_queries": 0,
        }

    def _get_content_hash(self, content: Union[str, bytes]) -> str:
        """Generate hash for content to use as cache key."""
        if isinstance(content, str):
            content = content.encode()
        return hashlib.sha1(content).hexdigest()

    @functools.lru_cache(maxsize=lru_cache_limit)
    def _cached_parse(
        self, content_hash: str, content: str | bytes
    ) -> etree.ElementBase:
        """Parse HTML content with caching.

        Args:
            content_hash: Hash of content for cache key
            content: HTML content to parse

        Returns:
            Parsed HTML tree

        Raises:
            ParserError: If parsing fails
        """
        try:
            if len(content) > self.max_document_size:
                self.stats["size_errors"] += 1
                raise ParserError(
                    f"Document size {len(content)} exceeds limit {self.max_document_size}"
                )

            return fromstring(content, parser=self._parser)

        except etree.ParserError as e:
            self.stats["parse_errors"] += 1
            raise ParserError(f"Failed to parse HTML: {str(e)}")
        except Exception as e:
            self.stats["parse_errors"] += 1
            raise ParserError(f"Unexpected parsing error: {str(e)}")

    def parse(
        self, content: Union[str, bytes], encoding: Optional[str] = None
    ) -> etree.ElementBase:
        """Parse HTML content with error handling.

        Args:
            content: HTML content to parse
            encoding: Optional encoding override

        Returns:
            Parsed HTML tree

        Raises:
            ParserError: If parsing fails
        """
        self.stats["total_parses"] += 1

        if isinstance(content, bytes):
            try:
                content = content.decode(encoding or self.encoding)
            except UnicodeDecodeError as e:
                self.stats["encoding_errors"] += 1
                self.logger.warning(
                    f"Encoding error: {str(e)}, trying to detect encoding..."
                )
                try:
                    # try chardet
                    if isinstance(content, bytes):
                        import charset_normalizer as chardet

                        detected = chardet.detect(content)
                        if detected["confidence"] > 0.8:
                            content = content.decode(detected["encoding"])
                            self.logger.info(
                                f"Decoded with detected encoding {detected['encoding']}"
                            )
                        else:
                            raise ParserError(
                                f"Could not detect content encoding (confidence: {detected['confidence']})"
                            )

                except ImportError:
                    self.logger.error("chardet not installed, cannot detect encoding")
                    raise ParserError(
                        "Failed to decode content and chardet not available"
                    )

        try:
            content_hash = self._get_content_hash(content)
            tree = self._cached_parse(content_hash, content)
            self.stats["cache_hits"] += 1
            return tree

        except Exception as e:
            self.logger.error(f"Parsing error: {str(e)}")
            raise

    def _get_cached_css(self, selector: str) -> Any:
        """Get cached CSS selector."""
        if selector not in self._css_cache:
            self._css_cache[selector] = CSSSelector(selector)
        return self._css_cache[selector]

    def _get_cached_xpath(self, xpath: str) -> Any:
        """Get cached XPath expression."""
        if xpath not in self._xpath_cache:
            self._xpath_cache[xpath] = etree.XPath(xpath)
        return self._xpath_cache[xpath]

    def css(self, tree: etree.ElementBase, selector: str) -> list:
        """Extract elements using CSS selector.

        Args:
            tree: Parsed HTML tree
            selector: CSS selector string

        Returns:
            List of matching elements
        """
        self.stats["total_css_queries"] += 1
        try:
            css_selector = self._get_cached_css(selector)
            self.stats["css_cache_hits"] += 1
            return css_selector(tree)
        except Exception as e:
            self.logger.error(f"CSS selector error: {str(e)}")
            return []

    def xpath(self, tree: etree.ElementBase, xpath: str) -> list:
        """Extract elements using XPath.

        Args:
            tree: Parsed HTML tree
            xpath: XPath string

        Returns:
            List of matching elements
        """
        self.stats["total_xpath_queries"] += 1
        try:
            xpath_expr = self._get_cached_xpath(xpath)
            self.stats["xpath_cache_hits"] += 1
            return xpath_expr(tree)
        except Exception as e:
            self.logger.error(f"XPath error: {str(e)}")
            return []

    def extract(
        self,
        content: Union[str, bytes],
        extractor: Union[str, Callable],
        encoding: Optional[str] = None,
    ) -> Any:
        """Parse and extract data in one step.

        Args:
            content: HTML content
            extractor: CSS selector, XPath or callback function
            encoding: Optional encoding override

        Returns:
            Extracted data
        """
        tree = self.parse(content, encoding)

        if isinstance(extractor, str):
            if extractor.startswith("//"):
                return self.xpath(tree, extractor)
            else:
                return self.css(tree, extractor)
        elif callable(extractor):
            return extractor(tree)
        else:
            raise ValueError("Extractor must be string or callable")

    def get_stats(self) -> dict:
        """Get parser statistics.

        Returns:
            Dictionary with parser statistics
        """
        return {
            **self.stats,
            "cache_info": self._cached_parse.cache_info()._asdict(),
            "css_cache_size": len(self._css_cache),
            "xpath_cache_size": len(self._xpath_cache),
        }

    def clear_caches(self) -> None:
        """Clear all parser caches."""
        self._cached_parse.cache_clear()
        self._css_cache.clear()
        self._xpath_cache.clear()
        self.logger.info("All parser caches cleared")
