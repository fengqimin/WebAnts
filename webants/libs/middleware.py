"""MiddleWare

使用Chain of Responsibility模式
"""

from abc import abstractmethod
from collections import defaultdict

mw_attrs = ['process_request', 'process_response']


class Middleware:
    @abstractmethod
    def handle(self, obj):
        pass


class MiddlewareManager:
    mw_attrs = ['process_request', 'process_response']

    def __init__(self, *mws):
        self.middlewares = defaultdict(list)
        for mw in mws:
            self._add_middleware(mw)

    def _add_middleware(self, middleware: Middleware):
        for mw_attr in self.mw_attrs:
            if hasattr(middleware, mw_attr):
                self.middlewares[mw_attr].append(middleware)
