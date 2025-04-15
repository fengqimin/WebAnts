"""
Exception class

此模块定义了一系列自定义异常类，用于在 Web 爬虫框架中处理各种异常情况。
"""

# 定义模块中可以被外部导入的所有名称
__all__ = [
    'InvalidDownloader',
    'InvalidExtractor',
    'InvalidParser',
    "InvalidScheduler",
    'InvalidRequestMethod',
    'InvalidURL',
    'NotAbsoluteURLError',
]

# 定义一个无效下载器异常类，继承自内置的 Exception 类
class InvalidDownloader(Exception):
    """
    当使用的下载器无效时抛出此异常。
    """
    pass

# 定义一个无效提取器异常类，继承自内置的 Exception 类
class InvalidExtractor(Exception):
    """
    当使用的提取器无效时抛出此异常。
    """
    pass

# 定义一个无效解析器异常类，继承自内置的 Exception 类
class InvalidParser(Exception):
    """
    当使用的解析器无效时抛出此异常。
    """
    pass

# 定义一个无效请求方法异常类，继承自内置的 Exception 类
class InvalidRequestMethod(Exception):
    """
    当使用的请求方法无效时抛出此异常。
    """
    pass

# 定义一个无效调度器异常类，继承自内置的 Exception 类
class InvalidScheduler(Exception):
    """
    当使用的调度器无效时抛出此异常。
    """
    pass

# 定义一个无效 URL 异常类，继承自内置的 Exception 类
class InvalidURL(Exception):
    """
    当使用的 URL 无效时抛出此异常。
    """
    pass

# 定义一个非绝对 URL 错误异常类，继承自 InvalidURL 类
class NotAbsoluteURLError(InvalidURL):
    """
    当使用的 URL 不是绝对 URL 时抛出此异常。
    """
    pass
