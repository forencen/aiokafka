# encoding: utf-8

"""
@author: forencen
@time: 2020/11/25 6:10 下午
@desc: aio kafka 管理者
"""
import asyncio
from loguru import logger
import traceback

from aio_consumer import AioConsumer
from aio_producer import AioProducer
from redis_helper import RedisHelper
from message import Message


class KafkaManager:

    def __init__(self, config, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        redis_url = config.pop('KAFKA_REDIS_URL', None)
        assert redis_url is not None, 'kafka server need redis config: KAFKA_REDIS_URL'
        self.kafka_redis = RedisHelper(redis_url, loop=loop)
        self._waiting_publish_message_queue = config.pop(
            'WAITING_PUBLISH_MESSAGE_QUEUE', None) or 'kafka_waiting_publish_message'
        self._consumers_name_set = set()
        self._consumers = []
        self._producer_count = config.pop('PRODUCER_COUNT', None) or 5
        self._producers = []
        self.config = config
        # 'INIT' -> 'RUNNING' -> 'STOP'
        self.status = 'INIT'

    def register_consumer(self, consumer: AioConsumer):
        if not consumer:
            return
        if consumer.name in self._consumers_name_set:
            logger.warning(f'{consumer.name} already exists')
            return
        self._consumers.append(consumer)
        self._consumers_name_set.add(consumer.name)
        logger.info(f'{consumer.name} has already been registered')
        return

    def init_producer(self):
        for no in range(self._producer_count):
            self._producers.append(AioProducer(config=self.config,
                                               kafka_redis=self.kafka_redis,
                                               message_queue_key=self._waiting_publish_message_queue,
                                               loop=self.loop))

    def start(self):
        self.status = 'RUNNING'
        # start producer
        self.init_producer()
        for producer in self._producers:
            producer.status = 'RUNNING'
            self.loop.create_task(producer.run())

        # start consumer
        for consumer in self._consumers:
            consumer.status = 'RUNNING'
            self.loop.create_task(consumer.run())

    def stop(self):
        self.status = 'STOP'
        for producer in self._producers:
            producer.status = 'STOP'

        for consumer in self._consumers:
            consumer.status = 'STOP'
        self._consumers_name_set = set()

        self.loop.call_soon(self.kafka_redis.close)

    async def publish_message(self, topic, data, timely=False):
        """
        发送消息
        @param topic: 指定topic
        @param data: 需要发送的数据 ps:能序列化的数据
        @param timely: false：按照消息提交顺序投递 true：尽快投递
        @return: 消息是否发送成功
        """
        if self.status == 'RUNNING':
            try:
                with await self.kafka_redis.pool as p:
                    message_str = Message(topic=topic, content=data).dumps()
                    if timely:
                        count = await p.lpush(self._waiting_publish_message_queue, message_str)
                    else:
                        count = await p.rpush(self._waiting_publish_message_queue, message_str)
                    return count > 0
            except Exception as e:
                logger.error(f'publish msg to redis error, error:{e.args}.{traceback.format_exc()}')
        else:
            logger.error('kafka server waiting start')
        return False

    def publish_message_thread(self, topic, data, timely=False):
        return asyncio.run_coroutine_threadsafe(self.publish_message(topic=topic, data=data, timely=timely), self.loop)
