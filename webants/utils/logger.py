# -*- coding: utf-8 -*-
import logging

__all__ = [
    'get_logger',
]


def get_logger(log_name: str,
               log_file=None,
               log_level=logging.INFO,
               file_handler_level=logging.INFO,
               stream_handler_level=logging.DEBUG):
    """设置日志格式

    :param log_level:
    :param log_name:
    :param log_file:
    :param file_handler_level:
    :param stream_handler_level:
    :return:
    """
    _logger = logging.getLogger(log_name.upper())
    # logging.basicConfig()

    if log_file:
        # 创建文件处理器
        file_handler = logging.FileHandler(filename=log_file, encoding='utf-8')
        # 将被替换为
        file_handler.setLevel(file_handler_level)

        # 定义输出格式
        file_formatter = logging.Formatter('%(lineno)4d - %(asctime)s - %(message)s')
        file_handler.setFormatter(file_formatter)

        # 将创建的文件和流处理器添加logger中
        _logger.addHandler(file_handler)

    # 创建流输出处理器，用于输出
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(stream_handler_level)

    # 定义输出格式
    stream_formatter = logging.Formatter('%(asctime)s:[%(name)s]%(levelname)s:%(message)s')
    stream_handler.setFormatter(stream_formatter)

    # 将创建的流处理器添加logger中
    _logger.addHandler(stream_handler)

    _logger.setLevel(log_level)
    return _logger
