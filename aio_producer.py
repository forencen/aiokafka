# encoding: utf-8

"""
@author: forencen
@time: 2020/11/25 6:11 下午
@desc: 异步生产者
"""
import asyncio
import traceback
from threading import Thread

from confluent_kafka.cimpl import Producer, KafkaException
from loguru import logger

from message import Message


class AioProducer:

    def __init__(self, config, kafka_redis, message_queue_key, name=None, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        assert config is not None, 'init kafka product error, config is None'
        self.kafka_redis = kafka_redis
        self.message_queue_key = message_queue_key
        self._producer = Producer(**config)
        # 'INIT' -> 'RUNNING' -> 'STOP'
        self.status = 'INIT'
        self.name = name
        self.__heath_check = Thread(target=self.__producer_health_loop)
        self.__heath_check.setDaemon(True)
        self.__heath_check.start()

    def __producer_health_loop(self):
        while self.status != 'STOP':
            self._producer.poll(1)

    def stop(self):
        self.status = 'STOP'

    def close(self):
        """
        kafka生产者的poll()方法使用异步方式进行数据推送，当程序结束的时候，不能保证数据已经完成推送。
        因此需要在结束生产者之前，使用flush()方法将未推送的数据已同步方式推送完成，等待推送完成后再结束进程。
        :return:
        """
        try:
            self.__heath_check.join()
            self._producer.flush()
        except Exception as e:
            logger.error(f'{self.name} close error: {e.args}, traceback: {traceback.format_exc()}')

    async def publish(self, topic: str, message: str):
        """
        kafka生产者主函数，将传入的数据data推送到指定topic中, 并在推送完成后调用callback回调函数

        :param topic:     推送数据的kafka主题
        :param message:   推送数据 str
        :return: 是否推送成功 True/False
        """
        result = self.loop.create_future()

        def ack(err, msg):
            """  成功/失败的处理函数 """
            if err is not None:
                logger.error(f'{message} delivery failed: {err}')
                self.loop.call_soon_threadsafe(result.set_result, False)
            else:
                logger.info(f'{message} delivered to {msg.topic()} partition:[{msg.partition()}]')
                self.loop.call_soon_threadsafe(result.set_result, True)

        try:
            self._producer.produce(topic, message, on_delivery=ack)
            return await result
        except BufferError as e:
            logger.error('Local producer queue is full ({} messages awaiting delivery): try again\n'.format(
                len(self._producer)))
            await asyncio.sleep(1)
        except KafkaException as e:
            logger.error(f'producer publish {message} error, '
                         f'topic:{topic}.error_info: {e.args[0]}')
        except Exception as e:
            logger.error(f'producer publish {message} error'
                         f'topic:{topic}.error_info: {traceback.format_exc()}', exc_info=e)
        return False

    async def __get_message(self) -> str:
        try:
            with await self.kafka_redis.pool as p:
                return await p.lpop(self.message_queue_key)
        except TimeoutError:
            logger.info(f'redis_key:{self.message_queue_key} timeout')
        return ''

    async def __retry_message(self, message):
        try:
            with await self.kafka_redis.pool as p:
                return await p.rpush(self.message_queue_key, message)
        except TimeoutError:
            logger.info(f'redis_key:{self.message_queue_key} timeout')
        return 0

    async def run(self):
        while self.status == 'RUNNING':
            message = Message.loads(await self.__get_message())
            if not message:
                await asyncio.sleep(1)
                continue
            flag = await self.publish(topic=message.topic, message=message.dumps())
            push_2_redis_flag = False
            while not flag and not push_2_redis_flag:
                message.delivery_retry()
                push_2_redis_flag = await self.__retry_message(message.dumps())
                if not push_2_redis_flag:
                    await asyncio.sleep(5)
        await self.loop.run_in_executor(None, self.close)
