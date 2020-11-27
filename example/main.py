# encoding: utf-8

"""
@author: forencen
@time: 2020/11/25 6:06 下午
@desc: aiokafka example
"""
import asyncio
import os
import threading

from flask import Flask

from example.common import registry
from example.config import CONFIG
from manager import KafkaManager
from tools import auto_load_kafka_handler

app = Flask(__name__)


def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


new_loop = asyncio.new_event_loop()
registry.set_kafka_server('loop', new_loop)


def init_kafka():
    kafka_manager = KafkaManager(config=CONFIG.get('KAFKA'), loop=new_loop)
    t = threading.Thread(target=start_loop, args=(new_loop,))
    t.setDaemon(True)
    t.start()
    registry.set_kafka_server('instance', kafka_manager)
    auto_load_kafka_handler(['handler'], os.path.dirname(os.path.abspath(__file__)))
    kafka_manager.start()


@app.route('/<data_info>')
def hello_world(data_info):
    registry.get_kafka_server('instance').publish_message_thread(topic='nb-test-flask-aiokafka', data={'id': data_info})
    return 'Hello, World!'


if __name__ == '__main__':
    init_kafka()
    app.run(debug=True)
