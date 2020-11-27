# encoding: utf-8

"""
@author: Forencen
@time: 2020/7/28 1:50 下午
@desc: 异步redis封装函数, 后续添加管道操作和布隆过滤器
"""
import asyncio

import aioredis


async def init_redis(redis_url: str):
    return await aioredis.create_redis_pool(redis_url)


class RedisHelper:

    def __init__(self, redis_url, loop=None, async_init=False):
        self.redis_url = redis_url
        self.status = 'INIT'
        if not loop:
            loop = asyncio.get_event_loop()
        if not async_init:
            self._pool = loop.run_until_complete(init_redis(redis_url))
            self.status = 'RUNNING'

    async def init(self):
        if self.status == 'INIT':
            self._pool = await init_redis(self.redis_url)
            self.status = 'RUNNING'

    @property
    def pool(self):
        return self._pool

    async def close(self):
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()

