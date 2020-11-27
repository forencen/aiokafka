# encoding: utf-8

"""
@author: forencen
@time: 2020/11/27 11:27 上午
@desc:
"""
from tools import handler


@handler(topics=['nb-test-flask-aiokafka'], group_id='local_5', max_retry=-1, count=1)
async def handler_test_topic(msg):
    print(msg)
