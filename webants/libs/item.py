from typing import Sequence, Callable

import lxml.etree

from webants.libs import (
    Link,
    ExtractorFactory,
    BaseExtractor,
    AttribExtractor,
    FilteringLinkExtractor,
    ElementExtractor,
)
from webants.utils.logger import get_logger

mod_logger = get_logger("Item", log_level=20)

__all__ = [
    "Item",
    "AttrItem",
    "ElementItem",
    "LinkItem",
    "TextItem",
    # temp
    "ItemDescriptor",
    "AttrItemDescriptor",
    "LinkItemDescriptor",
    "ElementItemDescriptor",
    "TextItemDescriptor",
]


class ExtractorDescriptor:
    """Descriptor class

    Manages access to Extractor instance data, directly returns extract results.
    https://docs.python.org/zh-cn/3.10/howto/descriptor.html

    Descriptors only work when used as class variables. They become ineffective when put into instances.
    """

    def __set_name__(self, owner, name):
        self._name = "_" + name

    def __get__(self, instance, owner):
        """Get the extraction result

        Args:
            instance: The instance
            owner: The owner class
            
        Returns:
            The extraction result
        """
        extractor = getattr(instance, self._name)
        mod_logger.debug(f"{type(extractor)}")

        return extractor.extract(instance.html)

    def __set__(self, instance, value):
        assert isinstance(
            value, BaseExtractor
        ), f"Expected class <Extractor>, got {value.__class__.__name__}"
        setattr(instance, self._name, value)


class Item:
    """Item base class

    Uses Extractor class to get Items from HTML
    """

    # Data descriptor
    field = ExtractorDescriptor()
    html: str = ""


class AttrItem(Item):
    def __init__(
        self,
        attr: str,
        selector: str = None,
        xpath: str = None,
        tags: Sequence[str] | str = None,
        many: bool = True,
    ):
        self.field = AttribExtractor(
            attr=attr, selector=selector, xpath=xpath, tags=tags, many=many
        )


class LinkItem(Item):
    def __init__(
        self,
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
        regexps_allow: list[str] = None,
        regexps_deny: list[str] = None,
        lenient: bool = False,
        link_process_func: Callable[[Link], Link] = None,
        log_level: int = 20,
        many: bool = True,
        unique: bool = True,
    ):
        self.field = FilteringLinkExtractor(
            attr=attr,
            selector=selector,
            xpath=xpath,
            tags=tags,
            base_url=base_url,
            normalize=normalize,
            extensions_allow=extensions_allow,
            extensions_deny=extensions_deny,
            hosts_allow=hosts_allow,
            hosts_deny=hosts_deny,
            schemes_allow=schemes_allow,
            schemes_deny=schemes_deny,
            regexps_allow=regexps_allow,
            regexps_deny=regexps_deny,
            lenient=lenient,
            link_process_func=link_process_func,
            log_level=log_level,
            many=many,
            unique=unique,
        )


class ElementItem(Item):
    def __init__(
        self,
        selector: str = None,
        xpath: str = None,
        tags: Sequence[str] | str = None,
        attr: str = None,
        many: bool = True,
    ):
        self.field = ExtractorFactory.create_extractor("ElementExtractor")(
            attr=attr, selector=selector, xpath=xpath, tags=tags, many=many
        )


class TextItem(Item):
    def __init__(
        self,
        selector: str = None,
        xpath: str = None,
        tags: Sequence[str] | str = None,
        attr: str = None,
        iter_text: bool = True,
        many: bool = True,
    ):
        self.field = ExtractorFactory.create_extractor("TextExtractor")(
            attr=attr,
            selector=selector,
            xpath=xpath,
            tags=tags,
            iter_text=iter_text,
            many=many,
        )


class ItemDescriptor:
    def __set_name__(self, owner, name):
        self._name = "_" + name

    def __get__(self, instance, owner):
        """Get the extraction result

        Args:
            instance: The instance
            owner: The owner class
            
        Returns:
            The extraction result
        """
        html = getattr(instance, self._name)

        return getattr(self, "extractor").extract(html)

    def __set__(self, instance, value):
        assert isinstance(
            value, (str, lxml.etree.ElementBase.__base__)
        ), f"Expected (str, etree._Element), got {value.__class__.__name__}"
        setattr(instance, self._name, value)


class AttrItemDescriptor(ItemDescriptor):
    def __init__(
        self,
        attr: str,
        selector: str = None,
        xpath: str = None,
        tags: Sequence[str] | str = None,
        many: bool = True,
    ):
        self.extractor = AttribExtractor(
            attr=attr, selector=selector, xpath=xpath, tags=tags, many=many
        )


class LinkItemDescriptor(ItemDescriptor):
    def __init__(
        self,
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
        regexps_allow: list[str] = None,
        regexps_deny: list[str] = None,
        lenient: bool = False,
        link_process_func: Callable[[Link], Link] = None,
        log_level: int = 20,
        many: bool = True,
        unique: bool = True,
    ):
        self.extractor = FilteringLinkExtractor(
            attr=attr,
            selector=selector,
            xpath=xpath,
            tags=tags,
            base_url=base_url,
            normalize=normalize,
            extensions_allow=extensions_allow,
            extensions_deny=extensions_deny,
            hosts_allow=hosts_allow,
            hosts_deny=hosts_deny,
            schemes_allow=schemes_allow,
            schemes_deny=schemes_deny,
            regexps_allow=regexps_allow,
            regexps_deny=regexps_deny,
            lenient=lenient,
            link_process_func=link_process_func,
            log_level=log_level,
            many=many,
            unique=unique,
        )


class ElementItemDescriptor(ItemDescriptor):
    def __init__(
        self,
        selector: str = None,
        xpath: str = None,
        tags: Sequence[str] | str = None,
        attr: str = None,
        many: bool = True,
    ):
        self.extractor = ElementExtractor(
            attr=attr, selector=selector, xpath=xpath, tags=tags, many=many
        )


class TextItemDescriptor(ItemDescriptor):
    def __init__(
        self,
        selector: str = None,
        xpath: str = None,
        tags: Sequence[str] | str = None,
        attr: str = None,
        iter_text: bool = True,
        many: bool = True,
    ):
        self.extractor = ExtractorFactory.create_extractor("TextExtractor")(
            attr=attr,
            selector=selector,
            xpath=xpath,
            tags=tags,
            iter_text=iter_text,
            many=many,
        )
