# encoding: utf-8

"""
@author: forencen
@time: 2020/11/25 6:11 下午
@desc: 异步消费者
"""
import asyncio
import copy
import json

from confluent_kafka.cimpl import Consumer, KafkaException
from loguru import logger


class AioConsumer:

    def __init__(self, config,
                 topics: list,
                 group_id: str,
                 handler,
                 max_retry=-1,
                 consumer_no=0,
                 timeout=1,
                 loop=None, exe=None):
        """
        consumer = new AioConsumer(...)
        :param config: kafka consumer config
        :param topics:
        :param group_id:
        :param handler:
        :param max_retry: 消费失败重试次数。-1：不重试
        :param consumer_no: 消费者编号
        :param timeout: poll超时时间
        :param loop:
        :param exe:
        """
        self.loop = loop or asyncio.get_event_loop()
        assert config is not None, 'init kafka consumer error, config is None'
        _config = copy.deepcopy(config)
        _config['group.id'] = group_id
        _config['on_commit'] = self.commit_completed
        self.handler = handler
        self.consumer = Consumer(_config)
        self.consumer.subscribe(topics)
        self.redis_retry_key = f'{"_".join(topics)}_{self.handler.__name__}'
        self.name = f'{self.redis_retry_key}_{consumer_no}'
        self.max_retry = max_retry
        self.exe = exe
        self.timeout = timeout
        # 'INIT' -> 'RUNNING' -> 'STOP'
        self.status = 'INIT'

    @staticmethod
    def commit_completed(err, partitions):
        if err:
            logger.info(str(err))
        else:
            logger.info("Committed partition offsets: " + str(partitions))

    async def poll(self):
        return await self.loop.run_in_executor(self.exe, self.consumer.poll, self.timeout)

    async def _get_message_from_kafka(self):
        poll_message = await self.poll()
        if not poll_message:
            return None
        elif poll_message.error():
            raise KafkaException(poll_message.error())
        else:
            return poll_message.value()

    async def run(self):
        while self.status == 'RUNNING':
            str_message = await self._get_message_from_kafka()
            message = json.loads(str_message or '{}')
            if not message:
                await asyncio.sleep(1)
                continue
            try:
                if asyncio.iscoroutinefunction(self.handler):
                    await self.handler(message)
                else:
                    self.handler(message)
                await self.commit()
            except Exception as e:
                logger.warning(f'{str(self)} handler error: {e.args}. msg: {str_message}')

        await self.close()

    async def commit(self):
        def _commit():
            self.consumer.commit(asynchronous=False)
        await self.loop.run_in_executor(self.exe, _commit)

    async def close(self):
        await self.commit()
        await self.loop.run_in_executor(self.exe, self.consumer.close)
        logger.info(f'{self.name} closed')

    def stop(self):
        self.status = 'STOP'





