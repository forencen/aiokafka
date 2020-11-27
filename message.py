# encoding: utf-8

"""
@author: forencen
@time: 2020/11/26 4:43 下午
@desc:
"""
import json
import time

from loguru import logger


class Message:

    def __init__(self, topic, content, timestamp=None, version=1, delivery_count=0, consumer_count=0):
        self.topic = topic
        self.content = content
        self.timestamp = timestamp or time.time()
        self.version = version
        self.delivery_count = delivery_count
        self.consumer_count = consumer_count

    @staticmethod
    def loads(message_obj):
        if not message_obj:
            return None
        message_obj = message_obj.decode()
        try:
            if isinstance(message_obj, str):
                message_obj = json.loads(message_obj)
            if isinstance(message_obj, dict):
                return Message(**message_obj)
            else:
                return None
        except Exception as e:
            logger.error(f'{message_obj} loads error: {e.args}')
            return None

    def dumps(self):
        res = self.__dict__
        return json.dumps(res, ensure_ascii=False).encode()

    def delivery_retry(self):
        self.delivery_count += 1

    def consumer_retry(self):
        self.consumer_count += 1
