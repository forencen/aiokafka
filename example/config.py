# encoding: utf-8

"""
@author: forencen
@time: 2020/11/26 5:57 下午
@desc:
"""
CONFIG = {
    "KAFKA": {
        "KAFKA_REDIS_URL": "redis://:test@127.0.0.1/6",  # your server
        "PRODUCER_COUNT": 1,
        "WAITING_PUBLISH_MESSAGE_QUEUE": "kafka_waiting_publish_message",
        "bootstrap.servers": ".....",  # your server
        "session.timeout.ms": 100000,
        "heartbeat.interval.ms": 30000,
        "enable.auto.commit": False,
        "max.poll.interval.ms": 500000,
        # "default.topic.config": {"auto.offset.reset": "latest"},
        "default.topic.config": {"auto.offset.reset": "earliest"},
        "queued.max.messages.kbytes": 2048576,
        "fetch.message.max.bytes": 1048576
    },
}
