"""Parser Module
This module provides HTML/XML parsing functionality with the following features:
- Efficient parsing using lxml
- CSS selector and XPath support
- Advanced caching mechanism using LRU cache
- Robust error handling with recovery
- Smart encoding detection
"""

import copy
import hashlib
import logging
from collections import OrderedDict
from typing import Any, Callable, Dict, Optional, Union

from lxml import etree
from lxml.cssselect import CSSSelector
from lxml.html import HTMLParser, fromstring

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
        cache_size: Optional[int] = None,
    ):
        """Initialize the parser.

        Args:
            encoding: Default encoding for parsing
            log_level: Logging level
            max_document_size: Maximum document size to parse in bytes
            cache_size: Maximum number of cached documents (0 disables caching)
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
        self.cache_size = lru_cache_limit if cache_size is None else max(0, int(cache_size))
        self._cache: Dict[str, etree.ElementBase] = OrderedDict()
        self._css_cache: Dict[str, Any] = {}
        self._xpath_cache: Dict[str, Any] = {}

        # Statistics
        self.stats = {
            "total_parses": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "parse_errors": 0,
            "encoding_errors": 0,
            "size_errors": 0,
            "css_cache_hits": 0,
            "css_cache_misses": 0,
            "xpath_cache_hits": 0,
            "xpath_cache_misses": 0,
            "total_css_queries": 0,
            "total_xpath_queries": 0,
        }

    def _get_content_hash(self, content: Union[str, bytes]) -> str:
        """Generate hash for content to use as cache key."""
        if isinstance(content, str):
            content = content.encode()
        return hashlib.sha1(content).hexdigest()

    def _parse_content(self, content: str | bytes) -> etree.ElementBase:
        """Parse HTML content without caching.

        Args:
            content: HTML content to parse

        Returns:
            Parsed HTML tree

        Raises:
            ParserError: If parsing fails
        """
        try:
            return fromstring(content, parser=self._parser)

        except etree.ParserError as e:
            self.stats["parse_errors"] += 1
            self.logger.error(f"HTML parsing error: {str(e)}")
            raise ParserError(f"Failed to parse HTML: {str(e)}")
        except Exception as e:
            self.stats["parse_errors"] += 1
            self.logger.error(f"Unexpected parsing error: {str(e)}")
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
                self.logger.debug(f"Decoded content with encoding {encoding}")
            except UnicodeDecodeError as e:
                self.stats["encoding_errors"] += 1
                self.logger.warning(f"Encoding error: {str(e)}, trying to detect encoding...")
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
                    raise ParserError("Failed to decode content and chardet not available")

        if len(content) > self.max_document_size:
            self.stats["size_errors"] += 1
            raise ParserError(
                f"Document size {len(content)} exceeds limit {self.max_document_size}"
            )

        content_hash = self._get_content_hash(content)
        self.logger.debug(f"Content hash: {content_hash}")

        if self.cache_size > 0:
            cached = self._cache.get(content_hash)
            if cached is not None:
                self._cache.move_to_end(content_hash)
                self.stats["cache_hits"] += 1
                return copy.deepcopy(cached)

        try:
            tree = self._parse_content(content)
        except Exception as e:
            self.logger.error(f"Parsing error: {str(e)}")
            raise

        self.stats["cache_misses"] += 1
        if self.cache_size <= 0:
            return tree

        self._cache[content_hash] = tree
        self._cache.move_to_end(content_hash)
        while len(self._cache) > self.cache_size:
            self._cache.popitem(last=False)
        return copy.deepcopy(tree)

    def _get_cached_css(self, selector: str) -> Any:
        """Get cached CSS selector."""
        if selector not in self._css_cache:
            self._css_cache[selector] = CSSSelector(selector)
            self.stats["css_cache_misses"] += 1
        else:
            self.stats["css_cache_hits"] += 1
        return self._css_cache[selector]

    def _get_cached_xpath(self, xpath: str) -> etree.XPath:
        """Get cached XPath expression."""
        if xpath not in self._xpath_cache:
            self._xpath_cache[xpath] = etree.XPath(xpath)
            self.stats["xpath_cache_misses"] += 1
        else:
            self.stats["xpath_cache_hits"] += 1
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
            return xpath_expr(tree)
        except Exception as e:
            self.logger.error(f"XPath error: {str(e)}")
            return []

    def extract(
        self,
        content: Union[str, bytes, etree.ElementBase],
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
        tree = self.parse(content, encoding) if isinstance(content, (str, bytes)) else content

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
            "cache_info": {
                "hits": self.stats["cache_hits"],
                "misses": self.stats["cache_misses"],
                "maxsize": self.cache_size,
                "currsize": len(self._cache),
            },
            "css_cache_size": len(self._css_cache),
            "xpath_cache_size": len(self._xpath_cache),
        }

    def clear_caches(self) -> None:
        """Clear all parser caches."""
        self._cache.clear()
        self._css_cache.clear()
        self._xpath_cache.clear()
        self.logger.info("All parser caches cleared")
