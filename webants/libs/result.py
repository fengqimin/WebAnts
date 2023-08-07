"""

"""


class Field(dict):
    """Field of Result class

    """
    pass


class Result:
    """Result class
    """
    __slots__ = 'spider', 'field', 'status', 'url', 'mediatype', 'title', 'crawl_time'

    def __init__(self,
                 spider: str,  # new, not changeable
                 field: Field,  #
                 status: int = 200,
                 url: str | None = None,
                 mediatype: str | None = None,
                 title: str | None = None,
                 crawl_time: float = 0.0,
                 ):
        self.spider = spider
        self.field = field
        self.status = status
        self.url = url
        self.mediatype = mediatype
        self.title = title
        self.crawl_time = crawl_time

    def __repr__(self):
        return f"<Result {self.url} [{self.title}]>"

    def save(self):
        pass
