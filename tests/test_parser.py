import pytest
from lxml import etree
from webants.parser import Parser
from webants.libs.exceptions import ParserError


@pytest.fixture
def parser():
    """Create a parser instance for testing."""
    return Parser(encoding="utf-8", log_level=10)


def test_parser_initialization(parser):
    """Test parser initialization with default settings."""
    assert parser.encoding == "utf-8"
    assert parser.max_document_size == 10 * 1024 * 1024


@pytest.mark.parametrize("encoding", ["utf-8", "ascii", "iso-8859-1"])
def test_parser_different_encodings(encoding, sample_html):
    """Test parser with different encodings."""
    parser = Parser(encoding=encoding)
    tree = parser.parse(sample_html)
    assert isinstance(tree, etree._Element)
    assert tree.find(".//title").text == "Test Page"


def test_parser_css_selector(parser, sample_html):
    """Test CSS selector functionality."""
    tree = parser.parse(sample_html)
    elements = parser.css(tree, "div.content a")
    assert len(elements) == 1
    assert elements[0].get("href") == "http://example.com"


def test_parser_xpath_selector(parser, sample_html):
    """Test XPath selector functionality."""
    tree = parser.parse(sample_html)
    elements = parser.xpath(tree, "//p[@class='text']")
    assert len(elements) == 1
    assert elements[0].text == "Sample Text"


def test_parser_invalid_html(parser):
    """Test parser behavior with invalid HTML."""
    invalid_html = "<div>Unclosed div"
    tree = parser.parse(invalid_html)
    assert isinstance(tree, etree._Element)


def test_parser_empty_content(parser):
    """Test parser behavior with empty content."""
    with pytest.raises(ParserError):
        parser.parse("")


def test_parser_oversize_content(parser):
    """Test parser behavior with content exceeding size limit."""
    parser.max_document_size = 10  # Set very small limit
    large_content = "a" * 100
    with pytest.raises(ParserError):
        parser.parse(large_content)


@pytest.mark.parametrize("selector", ["div.content", "a[href]", "#main p.text"])
def test_parser_css_cache(parser, sample_html, selector):
    """Test CSS selector caching mechanism."""
    tree = parser.parse(sample_html)
    # Call twice to test cache
    result1 = parser.css(tree, selector)
    result2 = parser.css(tree, selector)
    assert result1 == result2


def test_parser_extract_combined(parser, sample_html):
    """Test combined extraction using CSS and XPath."""
    tree = parser.parse(sample_html)
    link = parser.css(tree, "a")[0].get("href")
    text = parser.xpath(tree, "//p/text()")[0]
    assert link == "http://example.com"
    assert text == "Sample Text"


def test_parser_encoding_error(parser):
    """Test parser behavior with encoding errors."""
    invalid_utf8 = b"\xff\xfe\x00\x00"  # Invalid UTF-8 sequence
    with pytest.raises(ParserError):
        parser.parse(invalid_utf8)


def test_parser_large_content(parser):
    """Test parser with large content."""
    large_content = "a" * 1000000  # 1MB
    tree = parser.parse(large_content)
    assert isinstance(tree, etree._Element)


def test_parser_stats(parser, sample_html):
    """Test parser statistics."""
    e = parser.parse(sample_html)
    parser.css(e, "div.content a")
    parser.xpath(e, "//p")

    stats = parser.get_stats()
    assert stats["total_parses"] == 1
    assert stats["cache_hits"] == 1
    assert stats["parse_errors"] == 0
    assert stats["encoding_errors"] == 0
    assert stats["size_errors"] == 0
    assert stats["css_cache_hits"] == 1
    assert stats["xpath_cache_hits"] == 1
    assert stats["total_css_queries"] == 1
    assert stats["total_xpath_queries"] == 1


def test_parser_advanced_caching(parser, sample_html):
    """Test advanced caching mechanism."""

    # Test cache limit
    parser._cached_parse.cache_clear()  # Clear cache
    for _ in range(parser._cached_parse.cache_info().maxsize + 1):
        html = f""""
                <html>
                    <head><title>Test Page</title></head>
                    <body>
                        <div class="content">
                            <a href="http://example.com">Example Link</a>
                            <p class="text">Sample Text{_}</p>
                        </div>
                    </body>
                </html>
                """
        parser.parse(html)
    assert (
        parser._cached_parse.cache_info().currsize
        == parser._cached_parse.cache_info().maxsize
    )

    parser.parse(sample_html)
    assert (
        parser._cached_parse.cache_info().currsize
        == parser._cached_parse.cache_info().maxsize
    )

    # Test cache eviction
    parser.clear_caches()  # Clear cache
    for _ in range(parser._cached_parse.cache_info().maxsize + 1):
        parser.parse(sample_html)
    assert parser._cached_parse.cache_info().currsize == 1


def test_extract(parser, sample_html):
    """Test extraction functionality."""
    tree = parser.parse(sample_html)

    # xpath
    extracted_data = parser.extract(tree, extractor="//p")
    assert extracted_data is not None

    # css
    extracted_data = parser.extract(tree, extractor="p")
    assert extracted_data is not None

    extracted_data = parser.extract(tree, extractor=parser._get_cached_css("p"))
    assert extracted_data is not None

    with pytest.raises(ValueError):
        parser.extract(tree, extractor=123)
