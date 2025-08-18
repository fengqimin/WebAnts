import pytest
from lxml import etree
from webants.libs.extractor import (
    Link,
    ExtractorFactory,
    LinkExtractor,
    TextExtractor,
    ElementExtractor,
    AttribExtractor,
    FilteringLinkExtractor,
    iter_elements,
    find_elements,
    extract_attrib,
    extract_links,
    extract_text,
    get_base_url,
    get_html_title,
)


@pytest.fixture
def sample_html():
    return """
    <html>
        <head>
            <title>Test Page</title>
            <base href="http://example.com"/>
        </head>
        <body>
            <div class="content">
                <a href="/page1">Link 1</a>
                <a href="http://example.com/page2">Link 2</a>
                <p class="text">Sample Text</p>
                <img src="image.jpg" alt="Test Image"/>
            </div>
        </body>
    </html>
    """


@pytest.fixture
def html_tree(sample_html):
    return etree.HTML(sample_html)


def test_link_class():
    """Test Link class functionality"""
    link = Link("http://example.com", unique=True)
    assert link.url == "http://example.com"
    assert link.unique is True
    
    # Test remove_suffix method
    link = Link("http://example.com/index.html", unique=True)
    new_link = link.remove_suffix(".html")
    assert new_link.url == "http://example.com/index"
    assert new_link.unique is True


def test_iter_elements(html_tree):
    """Test iter_elements function"""
    # Test with single tag
    a_elements = list(iter_elements(html_tree, tags="a"))
    assert len(a_elements) == 2
    
    # Test with multiple tags
    elements = list(iter_elements(html_tree, tags=["a", "p"]))
    assert len(elements) == 3
    
    # Test with attribute filter
    img_elements = list(iter_elements(html_tree, tags="img", attr="alt"))
    assert len(img_elements) == 1


def test_find_elements(html_tree):
    """Test find_elements function"""
    # Test with CSS selector
    elements = find_elements(html_tree, selector="div.content a")
    assert len(elements) == 2
    
    # Test with XPath
    elements = find_elements(html_tree, xpath="//p[@class='text']")
    assert len(elements) == 1
    
    # Test with tags and attribute
    elements = find_elements(html_tree, tags="img", attr="src")
    assert len(elements) == 1


def test_extract_attrib(html_tree):
    """Test extract_attrib function"""
    # Test href attributes from links
    hrefs = extract_attrib(html_tree, "href", tags="a")
    assert len(hrefs) == 2
    assert "/page1" in hrefs
    
    # Test alt attribute from images
    alts = extract_attrib(html_tree, "alt", tags="img")
    assert len(alts) == 1
    assert "Test Image" in alts


def test_extract_links(html_tree):
    """Test extract_links function"""
    # Test without base_url
    links = extract_links(html_tree, tags="a")
    assert len(links) == 2
    
    # Test with base_url
    links = extract_links(html_tree, tags="a", base_url="http://example.com")
    assert len(links) == 2
    assert any(link.url == "http://example.com/page1" for link in links)


def test_extract_text(html_tree):
    """Test extract_text function"""
    # Test with specific tag
    texts = extract_text(html_tree, tags="p")
    assert "Sample Text" in texts
    
    # Test with CSS selector
    texts = extract_text(html_tree, selector="div.content p")
    assert "Sample Text" in texts


def test_get_base_url(html_tree):
    """Test get_base_url function"""
    base_url = get_base_url(html_tree)
    assert base_url == "http://example.com"


def test_get_html_title(html_tree):
    """Test get_html_title function"""
    title = get_html_title(html_tree)
    assert title == "Test Page"


def test_link_extractor():
    """Test LinkExtractor class"""
    extractor = LinkExtractor(base_url="http://example.com")
    html = """<div><a href="/page1">Link 1</a><a href="http://example.com/page2">Link 2</a></div>"""
    links = extractor.extract(html)
    assert len(links) == 2
    assert any(link.url == "http://example.com/page1" for link in links)
    assert any(link.url == "http://example.com/page2" for link in links)


def test_text_extractor():
    """Test TextExtractor class"""
    extractor = TextExtractor(selector="p")
    html = """<div><p>Text 1</p><p>Text 2</p></div>"""
    texts = extractor.extract(html)
    assert len(texts) == 2
    assert "Text 1" in texts
    assert "Text 2" in texts


def test_element_extractor():
    """Test ElementExtractor class"""
    extractor = ElementExtractor(tags="p")
    html = """<div><p>Text 1</p><p>Text 2</p></div>"""
    elements = extractor.extract(html)
    assert len(elements) == 2
    assert all(el.tag == "p" for el in elements)


def test_attrib_extractor():
    """Test AttribExtractor class"""
    extractor = AttribExtractor(tags="img", attr="src")
    html = """<div><img src="image1.jpg"/><img src="image2.jpg"/></div>"""
    srcs = extractor.extract(html)
    assert len(srcs) == 2
    assert "image1.jpg" in srcs
    assert "image2.jpg" in srcs


def test_filtering_link_extractor():
    """Test FilteringLinkExtractor class"""
    extractor = FilteringLinkExtractor(
        base_url="http://example.com",
        hosts_allow=["example.com"],
        extensions_deny=["pdf", "zip"],
        schemes_allow=["http", "https"]
    )
    
    html = """
    <div>
        <a href="/page1.html">Link 1</a>
        <a href="http://example.com/doc.pdf">PDF</a>
        <a href="http://other.com/page">Other</a>
        <a href="ftp://example.com/file">FTP</a>
    </div>
    """
    
    links = extractor.extract(html)
    assert len(links) == 1  # Only /page1.html should be allowed
    assert links[0].url == "http://example.com/page1.html"


def test_extractor_factory():
    """Test ExtractorFactory class"""
    # Test creating different types of extractors
    link_extractor = ExtractorFactory.create_extractor("LinkExtractor")
    assert isinstance(link_extractor(), LinkExtractor)
    
    text_extractor = ExtractorFactory.create_extractor("TextExtractor")
    assert isinstance(text_extractor(), TextExtractor)
    
    with pytest.raises(Exception):
        ExtractorFactory.create_extractor("NonExistentExtractor")


@pytest.mark.parametrize("html,expected", [
    ("<p>Simple text</p>", ["Simple text"]),
    ("<div><p>Nested text</p></div>", ["Nested text"]),
    ("<p>Multiple</p><p>paragraphs</p>", ["Multiple", "paragraphs"]),
])
def test_text_extraction_variations(html, expected):
    """Test text extraction with different HTML structures"""
    extractor = TextExtractor(tags="p")
    results = extractor.extract(html)
    assert all(exp in results for exp in expected)


@pytest.mark.parametrize("url,unique,expected", [
    ("http://example.com", True, "http://example.com"),
    ("http://example.com/", True, "http://example.com/"),
    ("/relative/path", True, "/relative/path"),
])
def test_link_variations(url, unique, expected):
    """Test Link class with different URL patterns"""
    link = Link(url, unique=unique)
    assert link.url == expected
    assert link.unique is unique

