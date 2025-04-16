"""Parser Module
This module provides HTML/XML parsing functionality with the following features:
- Efficient parsing using lxml
- CSS selector and XPath support
- Caching mechanism for improved performance
- Robust error handling
- Response content encoding detection
"""

import functools
import logging
from typing import Any, Callable, Optional, Union

from lxml import etree
from lxml.html import HTMLParser, fromstring

from webants.libs.exceptions import ParserError
from webants.utils.logger import get_logger


class Parser:
    """HTML/XML Parser with caching and error handling."""

    def __init__(self, encoding: str = "utf-8", log_level: int = logging.INFO):
        """Initialize the parser.
        
        Args:
            encoding: Default encoding for parsing
            log_level: Logging level
        """
        self.encoding = encoding
        self.logger = get_logger(self.__class__.__name__, log_level=log_level)
        self._parser = HTMLParser(encoding=encoding)
        self._cache = {}  # Simple memory cache
        self.stats = {
            "total_parses": 0,
            "cache_hits": 0,
            "parse_errors": 0,
            "encoding_errors": 0
        }

    @functools.lru_cache(maxsize=1000)
    def _cached_parse(self, content: str) -> etree.ElementBase:
        """Parse HTML content with caching.
        
        Args:
            content: HTML content to parse
            
        Returns:
            Parsed HTML tree
        """
        try:
            return fromstring(content, parser=self._parser)
        except etree.ParserError as e:
            self.stats["parse_errors"] += 1
            raise ParserError(f"Failed to parse HTML: {str(e)}")

    def parse(self, content: Union[str, bytes], encoding: Optional[str] = None) -> etree.ElementBase:
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
                self.logger.warning(f"Encoding error: {str(e)}, trying to detect encoding...")
                try:
                    # Attempt to detect encoding
                    import chardet
                    detected = chardet.detect(content)
                    if detected["confidence"] > 0.8:
                        content = content.decode(detected["encoding"])
                    else:
                        raise ParserError("Could not detect content encoding")
                except ImportError:
                    self.logger.error("chardet not installed, cannot detect encoding")
                    raise ParserError("Failed to decode content and chardet not available")

        try:
            return self._cached_parse(content)
        except Exception as e:
            self.logger.error(f"Parsing error: {str(e)}")
            raise

    def css(self, tree: etree.ElementBase, selector: str) -> list:
        """Extract elements using CSS selector.
        
        Args:
            tree: Parsed HTML tree
            selector: CSS selector string
            
        Returns:
            List of matching elements
        """
        try:
            return tree.cssselect(selector)
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
        try:
            return tree.xpath(xpath)
        except Exception as e:
            self.logger.error(f"XPath error: {str(e)}")
            return []

    def extract(
        self, 
        content: Union[str, bytes], 
        extractor: Union[str, Callable],
        encoding: Optional[str] = None
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
            if extractor.startswith('//'):
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
            "cache_info": self._cached_parse.cache_info()._asdict()
        }
