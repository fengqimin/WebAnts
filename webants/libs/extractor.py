import re
from abc import abstractmethod
from collections.abc import Sequence
from typing import TypeVar, Any, Type, Callable
from urllib.parse import urlparse, urljoin

from lxml import etree
from lxml.cssselect import CSSSelector

from webants.libs import InvalidExtractor
from webants.utils.logger import get_logger
from webants.utils.misc import args_to_list
from webants.utils.url import lenient_host, normalize_url

__all__ = [
    "Link",
    # 函数
    "iter_elements",
    "find_elements",
    "extract_attrib",
    "extract_links",
    "extract_and_filter_links",
    "extract_text",
    "get_base_url",
    "get_html_title",
    "ExtractorFactory",  # Extractor工厂类
    "BaseExtractor",  # Extractor 基类
    #
    "AttribExtractor",
    "LinkExtractor",
    "ElementExtractor",
    "TextExtractor",
    "FilteringLinkExtractor",
]

_ET = TypeVar(
    "_ET", "ElementExtractor", "LinkExtractor", "MediaExtractor", "TextExtractor"
)


class Link:
    """Link class

    保存提取出的URL
    """

    __slots__ = ("url", "unique")

    def __init__(self, url: str, unique: bool = True):
        self.url = url
        # url是否唯一，如果为True，调度器进行去重操作
        self.unique = unique

    def __repr__(self):
        return f"<Link {self.url}>"

    def remove_suffix(self, __suffix: str) -> "Link":
        return Link(url=self.url.removesuffix(__suffix), unique=self.unique)


def iter_elements(
    html: etree._Element,
    *,
    tags: Sequence[str] | str = None,
    attr: str = None,
) -> list[etree.ElementBase]:
    """根据tags和attr，迭代所有符合要求的元素

    :param html:
    :param tags:
    :param attr:
    :return:
    """

    if not isinstance(tags, (list, tuple)):
        assert isinstance(tags, str)
        tags = [tags]

    for tag in html.iter(*tags):
        if attr is None:
            yield tag
        else:
            if attr in tag.attrib.keys():
                yield tag


def find_elements(
    html: etree._Element | str,
    *,
    selector: str = None,
    xpath: str = None,
    tags: Sequence[str] | str = None,
    attr: str = None,
) -> list[etree.ElementBase]:
    """查找所有符合要求的元素，返回元素列表

    :param html:
    :param selector:
    :param xpath:
    :param tags:
    :param attr:

    :return:
    """
    assert isinstance(
        html, (str, etree.ElementBase.__base__)
    ), f"Expected 'str' or 'etree._Element', got '{html.__class__.__name__}'"

    if isinstance(html, str):
        html = etree.HTML(html)
    # list of tag, attr
    tags = args_to_list(tags)
    # 更新实例属性

    if selector:
        return html.cssselect(selector)
    elif xpath:
        return html.xpath(xpath)
    else:
        return iter_elements(
            html,
            tags=tags,
            attr=attr,
        )


def extract_attrib(
    html: etree._Element | str,
    attr: str,
    *,
    selector: str = None,
    xpath: str = None,
    tags: Sequence[str] | str = None,
) -> list[Any]:
    """查找所有符合要求的元素属性，返回属性值列表

    :param html:
    :param tags:
    :param attr:
    :param selector:
    :param xpath:
    :return: dict
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
    """查找所有符合要求的URL，返回Link列表

    :param html:
    :param tags:
    :param attr: 默认为href，可以根据URL所对应的属性进行调整
    :param selector:
    :param xpath:
    :param base_url:
    :param unique: url是否唯一
    :return:
    """
    if base_url:
        assert isinstance(
            base_url, str
        ), f"Expected str, got {base_url.__class__.__name__}"
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
            url=urljoin(base_url, element.attrib.get(attr))
            if base_url
            else element.attrib.get(attr),
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
    """查找所有符合要求的文本，返回文本列表

    :param html:
    :param selector:
    :param xpath:
    :param tags:
    :param attr:
    :param iter_text:
    :return:
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
    extractors: dict[str, _ET] = dict()  # 通过元类记录具体类
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

    通过调用工厂类(Extractor)的注册方法，将整个类对象{按类名：类对象}存储在字典里面，
    工厂方法按类名获取到类对象，弥补需手动修改代码的缺点
    """

    def __new__(mcs, name: str, bases: tuple, attrs: dict):
        cls = super().__new__(mcs, name, bases, attrs)

        #  使用元类，自动注册具体产品类
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

    利用lxml库，从xml, html文档中提取特定(css, xpath or tags (and attrs) )的元素(Element)和内容（content）
    提取顺序为：css, xpath, tags
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

        :param selector: CSS 选择器
        :param xpath: XPath路径表达式
        :param tags: 元素标签序列
        :param attr: 元素属性
        :param many: 是否提取全部，默认为True，为False是只提取第一个
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
        """从单个元素提取信息，并返回结果

        :param element:
        :return:
        """
        pass

    def _find_elements(self, html: etree._Element) -> list[etree.ElementBase]:
        """查找到符合要求的所有元素

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

        assert isinstance(
            html, (str, etree.ElementBase.__base__)
        ), f"Expected 'str' or 'etree._Element', got '{html.__class__.__name__}'"

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

    从xml, html文档中提取特定元素的特定属性值
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

    从xml, html文档中提取超链接（hyperlink）
    默认提取所有<a> [href] 属性
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
            assert isinstance(
                self.base_url, str
            ), f"Expected str, got {self.base_url.__class__.__name__}"
            assert urlparse(
                self.base_url
            ).scheme, f"Expected absolute URL, got {self.base_url}"
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

    如果 css selector, xpath, tags, attr同时为None，则遍历xml， html中的所有元素
    """

    __slots__ = ("selector", "xpath", "tags", "attr")

    def _extract_element(self, element: etree._Element) -> Any:
        return element


class TextExtractor(_LxmlElementExtractor):
    """Text Extractor class

    从xml, html文档中提取元素中的文本(Text)
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

    从html页面中提取特定的元素，并进行过滤
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
