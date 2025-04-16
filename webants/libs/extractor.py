import re
from abc import abstractmethod
from collections.abc import Sequence
from typing import Generator, TypeVar, Any, Type, Callable
from urllib.parse import urlparse, urljoin

from lxml import etree
from lxml.cssselect import CSSSelector

from webants.libs.exceptions import InvalidExtractor
from webants.utils.logger import get_logger
from webants.utils.misc import args_to_list
from webants.utils.url import lenient_host, normalize_url

__all__ = [
    "Link",
    # Functions
    "iter_elements",
    "find_elements",
    "extract_attrib",
    "extract_links",
    "extract_and_filter_links",
    "extract_text",
    "get_base_url",
    "get_html_title",
    "ExtractorFactory",  # Extractor factory class
    "BaseExtractor",  # Base Extractor class
    #
    "AttribExtractor",
    "LinkExtractor",
    "ElementExtractor",
    "TextExtractor",
    "FilteringLinkExtractor",
]

_ET = TypeVar(
    "_ET", "ElementExtractor", "LinkExtractor", "TextExtractor"
)


class Link:
    """Link class

    Stores extracted URLs
    """

    __slots__ = ("url", "unique")

    def __init__(self, url: str, unique: bool = True):
        self.url = url
        # Whether URL must be unique, if True scheduler will deduplicate
        self.unique = unique

    def __repr__(self):
        return f"<Link {self.url}>"

    def remove_suffix(self, __suffix: str) -> "Link":
        return Link(url=self.url.removesuffix(__suffix), unique=self.unique)


def iter_elements(
    html: etree._Element,
    *,
    tags: Sequence[str] | str,
    attr: str = None,
) -> Generator[etree.ElementBase, None, None]:
    """Iterate through all elements matching tags and attr

    Args:
        html: HTML element to process
        tags: Tag names to match, can be single tag name or list of tag names
        attr: Attribute name to match, if None no attribute matching is done

    Returns:
        List of matching elements
    """
    # If tags is not a list or tuple, convert it to a single-element list
    if not isinstance(tags, (list, tuple)):
        assert isinstance(tags, str)
        tags = [tags]
    # Iterate through all elements in HTML matching tags
    for tag in html.iter(*tags):
        # If attr is None, return element directly
        if attr is None:
            yield tag
        # Otherwise, check if element has specified attribute
        else:
            if attr in tag.attrib:
                yield tag


def find_elements(
    html: etree._Element | str,
    *,
    selector: str = None,
    xpath: str = None,
    tags: Sequence[str] | str = None,
    attr: str = None,
) -> list[etree.ElementBase]:
    """
    Find all elements matching the criteria, return a list of elements

    Args:
        html: HTML content to search, can be a string or etree._Element object
        selector: CSS selector to find elements, default is None
        xpath: XPath expression to find elements, default is None
        tags: Element tag names, can be single tag name or list of tag names, default is None
        attr: Element attribute name to filter elements containing the specified attribute, default is None

    Returns:
        List of matching elements
    """
    # Check if html is of type str or etree.ElementBase.__base__
    assert isinstance(html, (str, etree.ElementBase.__base__)), (
        f"Expected 'str' or 'etree._Element', got '{html.__class__.__name__}'"
    )

    # If html is a string, convert it to etree._Element object
    if isinstance(html, str):
        html = etree.HTML(html)
    # Convert tags parameter to list form
    tags = args_to_list(tags)

    # If CSS selector is provided, use it to find elements
    if selector:
        return html.cssselect(selector)
    # If XPath expression is provided, use it to find elements
    elif xpath:
        return html.xpath(xpath)
    # If neither selector nor XPath is provided, use iter_elements function to find elements based on tags and attributes
    else:
        return list(
            iter_elements(
                html,
                tags=tags,
                attr=attr,
            )
        )


def extract_attrib(
    html: etree._Element | str,
    attr: str,
    *,
    selector: str = None,
    xpath: str = None,
    tags: Sequence[str] | str = None,
) -> list[Any]:
    """Find all elements matching the criteria and extract their attributes, return a list of attribute values

    Args:
        html: HTML content to search
        tags: Element tag names
        attr: Attribute name to extract
        selector: CSS selector
        xpath: XPath expression

    Returns:
        List of attribute values
    """

    results = []
    for element in find_elements(
        html,
        selector=selector,
        xpath=xpath,
        tags=tags,
        attr=attr,
    ):
        if value := element.attrib.get(attr):
            results.append(value)

    return results


def extract_links(
    html: etree._Element | str,
    attr: str = "href",
    *,
    selector: str = None,
    xpath: str = None,
    tags: Sequence[str] | str = None,
    base_url: str = None,
    unique: bool = True,
) -> list[Link]:
    """Find all matching URLs and return a list of Link objects

    Args:
        html: HTML content
        tags: Element tag names
        attr: Default is href, can be adjusted based on the attribute corresponding to the URL
        selector: CSS selector
        xpath: XPath expression
        base_url: Base URL for resolving relative URLs
        unique: Whether URLs should be unique

    Returns:
        List of Link objects
    """
    if base_url:
        assert isinstance(base_url, str), (
            f"Expected str, got {base_url.__class__.__name__}"
        )
        assert urlparse(base_url).scheme, f"Expected absolute URL, got {base_url}"

    results = []
    for element in find_elements(
        html,
        selector=selector,
        xpath=xpath,
        tags=tags,
        attr=attr,
    ):
        if link := Link(
            url=(
                urljoin(base_url, element.attrib.get(attr))
                if base_url
                else element.attrib.get(attr)
            ),
            unique=unique,
        ):
            results.append(link)
    return results


def extract_and_filter_links(
    html,
    *,
    selector: str = None,
    xpath: str = None,
    tags: list[str] = None,
    attr: str = "href",
    base_url: str = None,
    normalize: bool = True,
    extensions_deny: list[str] = None,
    extensions_allow: list[str] = None,
    hosts_allow: list[str] = None,
    hosts_deny: list[str] = None,
    schemes_allow: list[str] = None,
    schemes_deny: list[str] = None,
    link_process_func: Callable[[Link], Link] = None,
    unique: bool = True,
) -> list[Link]:
    extensions_deny = [_.lower() for _ in set(args_to_list(extensions_deny))]
    extensions_allow = [_.lower() for _ in set(args_to_list(extensions_allow))]

    hosts_allow = [_.lower() for _ in set(args_to_list(hosts_allow))]
    hosts_deny = [_.lower() for _ in set(args_to_list(hosts_deny))]

    schemes_allow = [_.lower() for _ in set(args_to_list(schemes_allow))]
    schemes_deny = [_.lower() for _ in set(args_to_list(schemes_deny))]

    link_process_func = link_process_func or (lambda x: x)

    def _extension_allowed(extension: str) -> bool:
        if not extension:
            return False
        if extensions_allow:
            return extension.lower() in extensions_allow
        else:
            return True

    def _extension_denied(extension: str) -> bool:
        if not extension:
            return False
        if extensions_deny:
            return extension.lower() in extensions_deny
        else:
            return False

    def _host_allowed(host: str) -> bool:
        if hosts_allow:
            return host.lower() in hosts_allow
        else:
            return True

    def _host_allowed_lenient(host: str) -> bool:
        if hosts_allow:
            hosts = [lenient_host(_) for _ in hosts_allow]
            return lenient_host(host.lower()) in hosts
        else:
            return False

    def _host_denied(host: str) -> bool:
        if hosts_deny:
            return host.lower() in hosts_deny
        else:
            return False

    def _host_denied_lenient(host: str) -> bool:
        if hosts_deny:
            hosts = [lenient_host(_) for _ in hosts_deny]
            return lenient_host(host.lower()) in hosts
        else:
            return False

    def _scheme_allowed(scheme: str) -> bool:
        if schemes_allow:
            return scheme.lower() in schemes_allow
        else:
            return True

    def _scheme_denied(scheme: str) -> bool:
        if schemes_allow:
            return scheme.lower() in schemes_deny
        else:
            return False

    def _link_allowed(link: Link) -> bool:
        if normalize:
            link = normalize_url(link.url)

        parts = urlparse(link)
        host = parts.hostname
        ext = parts.path.rsplit(".")[-1]
        scheme = parts.scheme

        if not host:
            return False
        if not scheme:
            return False

        if not _scheme_allowed(scheme):
            return False
        if _scheme_denied(scheme):
            return False

        if not _host_allowed(host):
            return False
        if not _host_allowed_lenient(host):
            return False
        if _host_denied(host):
            return False
        if _host_denied_lenient(host):
            return False

        if not _extension_allowed(ext):
            return False
        if _extension_denied(ext):
            return False

        return True

    links = [
        link_process_func(link)
        for link in extract_links(
            html,
            selector=selector,
            xpath=xpath,
            tags=tags,
            attr=attr,
            base_url=base_url,
        )
        if _link_allowed(link)
    ]
    if unique:
        return list(set(links))
    else:
        return links


def extract_text(
    html: etree._Element | str,
    *,
    selector: str = None,
    xpath: str = None,
    tags: Sequence[str] | str = None,
    attr: str = None,
    iter_text: bool = True,
) -> list[str]:
    """Find all matching text and return a list of text values

    Args:
        html: HTML content
        selector: CSS selector
        xpath: XPath expression
        tags: Element tag names
        attr: Element attribute name
        iter_text: Whether to iterate through text, default is True

    Returns:
        List of text values
    """

    results = []
    for element in find_elements(
        html,
        selector=selector,
        xpath=xpath,
        tags=tags,
        attr=attr,
    ):
        if iter_text:
            if isinstance(element, str):
                results.append(element)
            else:
                results.extend(list(element.itertext()))
        else:
            results.append(element.text)

    return results


def get_base_url(html) -> str | None:
    try:
        return extract_links(html, selector="base")[0].url
    except (IndexError, AttributeError):
        return None


def get_html_title(html: etree._Element | str) -> str | None:
    try:
        return extract_text(html, tags="title")[0]
    except IndexError:
        return None


class ExtractorFactory:
    """Extractor factory class"""

    __slots__ = ()
    extractors: dict[str, _ET] = dict()  # Record specific classes through metaclass
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def register(cls, name: str, obj: object):
        cls.extractors[name] = obj

    @classmethod
    def create_extractor(cls, cls_name: str) -> Type["_LxmlElementExtractor"]:
        # extractor = cls.extractors.get(cls_name)
        for k in cls.extractors.keys():
            if k.lower().startswith(cls_name.lower()):
                return cls.extractors.get(k)

        raise InvalidExtractor(f"Expected {list(cls.extractors)}, got '{cls_name}'.")


class _ExtractorMeta(type):
    """Extractor metaclass

    By calling the registration method of the factory class (Extractor),
    store the entire class object {by class name: class object} in a dictionary.
    The factory method retrieves the class object by class name,
    eliminating the need for manual code modification.
    """

    def __new__(mcs, name: str, bases: tuple, attrs: dict):
        cls = super().__new__(mcs, name, bases, attrs)

        # Use metaclass to automatically register specific product classes
        if not name.startswith("_"):
            ExtractorFactory.register(name, cls)
        return cls


class BaseExtractor(metaclass=_ExtractorMeta):
    """Extractor base class"""

    __slots__ = ()

    def __init__(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:

        :key selector: str ,
        :key xpath: str,
        :key tags: Sequence[str] | str,
        :key attr: str
        """
        pass

    @abstractmethod
    def extract(self, html: etree._Element | str) -> list[Any]:
        pass


class _LxmlElementExtractor(BaseExtractor):
    """LxmlElementExtractor base class

    Use lxml library to extract specific elements (Element) and content from xml, html documents
    based on css, xpath or tags (and attrs). Extraction order: css, xpath, tags.
    """

    __slots__ = (
        "selector",
        "xpath",
        "tags",
        "attr",
        "css_selector",
        "xpath_expr",
        "many",
        "logger",
    )

    def __init__(
        self,
        selector: str = None,
        xpath: str = None,
        tags: Sequence[str] | str = None,
        attr: str = None,
        many: bool = True,
        **kwargs,
    ):
        """

        :param selector: CSS selector
        :param xpath: XPath expression
        :param tags: Element tag sequence
        :param attr: Element attribute
        :param many: Whether to extract all, default is True. If False, only the first one is extracted.
        :param kwargs:
        """
        super(_LxmlElementExtractor, self).__init__()
        # css selector expression
        self.selector = selector
        if selector and isinstance(selector, str):
            self.css_selector = CSSSelector(selector)
        else:
            self.css_selector = None
        # xpath expression
        self.xpath = xpath
        if xpath and isinstance(xpath, str):
            self.xpath_expr = etree.XPath(xpath)
        else:
            self.xpath_expr = None
        # element tag
        self.tags = args_to_list(tags)
        # element attribute
        self.attr = attr
        self.many = many

        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def _extract_element(self, element: etree.ElementBase) -> Any:
        """Extract information from a single element and return the result

        :param element:
        :return:
        """
        pass

    def _find_elements(self, html: etree._Element) -> list[etree.ElementBase]:
        """Find all elements matching the criteria

        :param html:
        :return:
        """

        if self.selector:
            # results = html.cssselect(self.selector)
            results = self.css_selector(html)
        elif self.xpath:
            # results = html.xpath(self.xpath)
            results = self.xpath_expr(html)
        else:
            if self.attr:
                results = [
                    el for el in html.iter(*self.tags) if self.attr in el.attrib.keys()
                ]
            else:
                results = [el for el in html.iter(*self.tags)]

        return results

    def extract(self, html: etree._Element | str) -> list[Any]:
        """Run the CSS，XPath expression on this etree or
        iterates over all elements with specific tags and attrs,
        returning a list of the results.

        :param html: xml, html document
        :return: a list of the results
        """
        if html is None:
            return []

        assert isinstance(html, (str, etree.ElementBase.__base__)), (
            f"Expected 'str' or 'etree._Element', got '{html.__class__.__name__}'"
        )

        if isinstance(html, str) and len(html) > 0:
            try:
                html = etree.HTML(html)
            except ValueError as e:
                self.logger.error(e)
                return []
        elif isinstance(html, etree.ElementBase.__base__):
            html = html
        else:
            return []

        results = []

        elements = self._find_elements(html)

        for element in elements:
            result = self._extract_element(element)
            if result is not None:
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)

        if self.many:
            return results
        else:
            return results[:1]


class AttribExtractor(_LxmlElementExtractor):
    """attribute Extractor class

    Extract specific attribute values from specific elements in xml, html documents
    """

    __slots__ = ("selector", "xpath", "tags", "attr")

    def __init__(
        self,
        selector: str = None,
        xpath: str = None,
        tags: Sequence[str] | str = None,
        attr: str = None,
        many: bool = True,
        **kwargs,
    ):
        assert attr, f"attr can't be {attr}."
        super().__init__(
            attr=attr, selector=selector, xpath=xpath, tags=tags, many=many, **kwargs
        )
        #
        # self.selector = selector
        # self.xpath = xpath
        # self.tags = args_to_list(tags)

    def _extract_element(
        self,
        element: etree._Element | str,
    ) -> str | None:
        """

        :param element:
        :return:
        """

        try:
            return element.attrib.get(self.attr)
        except AttribExtractor:
            return None


class LinkExtractor(AttribExtractor):
    """Link Extractor class

    Extract hyperlinks from xml, html documents
    Default extracts all <a> [href] attributes
    """

    __slots__ = "base_url", "unique"

    def __init__(
        self,
        base_url: str = None,
        selector: str = None,
        xpath: str = None,
        tags: Sequence[str] | str = "a",
        attr: str = "href",
        many: bool = True,
        unique: bool = True,
        **kwargs,
    ):
        super().__init__(
            selector=selector, xpath=xpath, tags=tags, attr=attr, many=many, **kwargs
        )

        self.base_url = base_url
        if self.base_url:
            assert isinstance(self.base_url, str), (
                f"Expected str, got {self.base_url.__class__.__name__}"
            )
            assert urlparse(self.base_url).scheme, (
                f"Expected absolute URL, got {self.base_url}"
            )
        self.unique = unique

    def _extract_element(self, element: etree._Element | str) -> Link | None:
        """

        :param element:
        :return:
        """
        try:
            return Link(
                url=urljoin(self.base_url, element.attrib.get(self.attr)),
                unique=self.unique,
            )
        except AttributeError:
            return None


class ElementExtractor(_LxmlElementExtractor):
    """Element Extractor class

    If css selector, xpath, tags, attr are all None, iterate through all elements in xml, html
    """

    __slots__ = ("selector", "xpath", "tags", "attr")

    def _extract_element(self, element: etree._Element) -> Any:
        return element


class TextExtractor(_LxmlElementExtractor):
    """Text Extractor class

    Extract text from elements in xml, html documents
    """

    __slots__ = ("selector", "xpath", "tags", "attr", "iter_text")

    def __init__(
        self,
        iter_text: bool = True,
        selector: str = None,
        xpath: str = None,
        tags: Sequence[str] | str = None,
        attr: str = None,
        many: bool = True,
        **kwargs,
    ):
        super().__init__(
            attr=attr, selector=selector, xpath=xpath, tags=tags, many=many, **kwargs
        )

        self.iter_text = iter_text

    def _extract_element(self, element: etree.ElementBase) -> list[str] | str | None:
        if self.iter_text:
            if isinstance(element, str):
                return element
            else:
                text = ""
                try:
                    text = element.itertext()
                except ValueError:
                    pass
                return list(text)
        else:
            return element.text


class FilteringLinkExtractor(BaseExtractor):
    """FilteringExtractor class

    Extract specific elements from html pages and filter them
    """

    def __init__(
        self,
        *,
        selector: str = None,
        xpath: str = None,
        tags: list[str] = None,
        attr: str = "href",
        base_url: str = None,
        normalize: bool = True,
        extensions_deny: list[str] = None,
        extensions_allow: list[str] = None,
        hosts_allow: list[str] = None,
        hosts_deny: list[str] = None,
        regexps_allow: list[str] = None,
        regexps_deny: list[str] = None,
        schemes_allow: list[str] = None,
        schemes_deny: list[str] = None,
        lenient: bool = False,
        link_process_func: Callable[[Link], Link] = None,
        log_level: int = 20,
        many: bool = True,
        unique: bool = True,
    ):
        super(FilteringLinkExtractor, self).__init__()
        self.normalize = normalize

        self.extensions_deny = [_.lower() for _ in set(args_to_list(extensions_deny))]
        self.extensions_allow = [_.lower() for _ in set(args_to_list(extensions_allow))]

        self.hosts_allow = [_.lower() for _ in set(args_to_list(hosts_allow))]
        self.hosts_deny = [_.lower() for _ in set(args_to_list(hosts_deny))]

        self.regexps_allow = [
            re.compile(_)
            for _ in set(args_to_list(regexps_allow))
            if isinstance(_, str)
        ]
        self.regexps_deny = [
            re.compile(_) for _ in set(args_to_list(regexps_deny)) if isinstance(_, str)
        ]

        self.schemes_allow = [_.lower() for _ in set(args_to_list(schemes_allow))]
        self.schemes_deny = [_.lower() for _ in set(args_to_list(schemes_deny))]

        self.link_extractor = LinkExtractor(
            selector=selector,
            xpath=xpath,
            tags=tags,
            attr=attr,
            base_url=base_url,
            many=many,
            unique=unique,
        )
        self.lenient = lenient

        self.link_process_func = link_process_func or (lambda x: x)
        self.logger = get_logger(self.__class__.__name__, log_level=log_level)

    def _extension_allowed(self, extension: str) -> bool:
        if self.extensions_allow:
            if not extension:
                return False
            return extension.lower() in self.extensions_allow
        else:
            return True

    def _extension_denied(self, extension: str) -> bool:
        if not extension:
            return False
        if self.extensions_deny:
            return extension.lower() in self.extensions_deny
        else:
            return False

    def _host_allowed(self, host: str) -> bool:
        if self.hosts_allow:
            return host.lower() in self.hosts_allow
        else:
            return True

    def _host_allowed_lenient(self, host: str) -> bool:
        if self.hosts_allow:
            hosts = [lenient_host(_) for _ in self.hosts_allow]
            return lenient_host(host.lower()) in hosts
        else:
            return False

    def _host_denied(self, host: str) -> bool:
        if self.hosts_deny:
            return host.lower() in self.hosts_deny
        else:
            return False

    def _host_denied_lenient(self, host: str) -> bool:
        if self.hosts_deny:
            hosts = [lenient_host(_) for _ in self.hosts_deny]
            return lenient_host(host.lower()) in hosts
        else:
            return False

    def _scheme_allowed(self, scheme: str) -> bool:
        if self.schemes_allow:
            return scheme.lower() in self.schemes_allow
        else:
            return True

    def _scheme_denied(self, scheme: str) -> bool:
        if self.schemes_deny:
            return scheme.lower() in self.schemes_deny
        else:
            return False

    def _regex_allowed(self, url: str) -> bool:
        if self.regexps_allow:
            return any(r.search(url) for r in self.regexps_allow)
        else:
            return True

    def _regex_denied(self, url: str) -> bool:
        if self.regexps_deny:
            return any(r.search(url) for r in self.regexps_deny)
        else:
            return False

    def link_allowed(self, link: Link) -> bool:
        if self.normalize:
            link = normalize_url(link.url)

        parts = urlparse(link)
        host = parts.hostname
        ext = parts.path.rsplit(".")[-1]
        scheme = parts.scheme
        self.logger.debug(f"{link} {ext}")

        if not host:
            return False
        if not scheme:
            return False

        if not self._scheme_allowed(scheme):
            return False
        if self._scheme_denied(scheme):
            return False

        if not self.lenient:
            if not self._host_allowed(host):
                return False
            if self._host_denied(host):
                return False
        else:
            if not self._host_allowed_lenient(host):
                return False
            if self._host_denied_lenient(host):
                return False

        if not self._extension_allowed(ext):
            return False
        if self._extension_denied(ext):
            return False

        if not self._regex_allowed(link):
            return False
        if self._regex_denied(link):
            return False

        return True

    def extract(
        self,
        html: etree._Element | str,
    ) -> list[Link]:
        """

        :param html:

        :return:
        """

        links = self.link_extractor.extract(html)
        self.logger.debug(f"origin: {links}")
        links = [
            self.link_process_func(link) for link in links if self.link_allowed(link)
        ]
        self.logger.debug(f"filtered: {links}")
        return links


class RegexExtractor(BaseExtractor):
    def extract(self, html: etree._Element | str) -> list[Any]:
        pass
