import pytest
from lxml import etree

from webants.libs.item import (
    AttrItem, LinkItem, ElementItem, TextItem,
    AttrItemDescriptor, LinkItemDescriptor, ElementItemDescriptor, TextItemDescriptor
)

HTML_STR = """
<html>
    <body>
        <div class="container">
            <a href="https://example.com" class="link">Link</a>
            <p class="text">Sample text</p>
            <img src="image.jpg" alt="Test image">
        </div>
    </body>
</html>
"""

@pytest.fixture
def html():
    return HTML_STR

def test_attr_item(html):
    item = AttrItem(attr="href", selector="a.link")
    item.html = html
    assert item.field == ["https://example.com"]

def test_link_item(html):
    item = LinkItem(selector="a.link")
    item.html = html
    links = item.field
    assert len(links) == 1
    assert links[0].url == "https://example.com"

def test_element_item(html):
    item = ElementItem(selector="p.text")
    item.html = html
    elements = item.field
    assert len(elements) == 1
    assert elements[0].text == "Sample text"

def test_text_item(html):
    item = TextItem(selector="p.text")
    item.html = html
    assert item.field == ["Sample text"]

class TestItem:
    attr_field = AttrItemDescriptor(attr="alt", selector="img")
    link_field = LinkItemDescriptor(selector="a.link")
    element_field = ElementItemDescriptor(selector="p.text")
    text_field = TextItemDescriptor(selector="p.text")

    def test_descriptors(self, html):
        self.html = html
        assert self.attr_field == ["Test image"]
        assert self.link_field[0].url == "https://example.com"
        assert self.element_field[0].text == "Sample text" 
        assert self.text_field == ["Sample text"]

def test_invalid_html():
    item = TextItem(selector="p")
    with pytest.raises(AssertionError):
        item.html = 123