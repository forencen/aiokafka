# encoding: utf-8

"""
@author: forencen
@time: 2020/11/25 6:13 下午
@desc:
"""
import os

from loguru import logger


def handler(topics, group_id=None, max_retry=-1, timeout=1, count=1, slf_config=None):
    """
    消费者装饰器
    @param count: 启动的消费者数量，默认为1
    @param topics: 监听的topic，可以是数组
    @param group_id: 消费者的分组，建议传递
    @param max_retry: 最大重试次数，-1是可以一种重试。如果消息重试次数大于max_retry，会被丢入死信队列
    @param timeout: 消费者poll超时时间，不指定为默认为30秒（指定的原因是：怀疑长时间时间没消息 一直等待poll 会导致消费者error）。
    @param slf_config:
    @return:

    """
    from example.common import registry
    from aio_consumer import AioConsumer

    def decorator(func):
        logger.info(f'register {func.__name__} to kafka server, topics: {topics}, '
                    f'group_id: {group_id}, timeout: {timeout}')
        kafka_manager = registry.get_kafka_server('instance')
        _config = kafka_manager.config
        if slf_config:
            _config.update(slf_config)
        [kafka_manager.register_consumer(AioConsumer(handler=func, group_id=group_id, timeout=timeout,
                                                     topics=topics, max_retry=max_retry, loop=kafka_manager.loop,
                                                     consumer_no=no, config=_config)) for no in range(count)]

    return decorator


def auto_load_kafka_handler(handler_files, dir_name):
    """
    自动加载kafka处理函数
    @param handler_files: 处理文件目录
    @param dir_name: 文件绝对路径
    :return:

    """
    for file_name in handler_files:
        split_file_name = file_name.split(".")
        temp = dir_name
        while split_file_name:
            try:
                file_path_name = split_file_name.pop(0)
            except IndexError as e:
                break
            if dir_name is not None:
                temp = os.path.join(temp, file_path_name)
        if os.path.isdir(temp):
            for item in os.listdir(temp):
                if item.endswith(".py"):
                    file_name = os.path.basename(file_name)
                    __import__(f"{file_name}.{item[:-3]}")
        elif os.path.exists(f"{temp}.py"):
            __import__(f"{file_name}")
        else:
            logger.warning(f"{file_name} can not import handler")
    logger.info('auto import completed')
