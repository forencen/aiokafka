# encoding: utf-8

"""
@author: forencen
@time: 2020/11/27 11:40 上午
@desc:
"""


class registry:
    def __init__(self):
        self._data = {}

    def set(self, k, v, group=None):
        if not group:
            group = '__default'
        if group not in self._data:
            self._data[group] = {}
        self._data[group][k] = v

    def get(self, k, group=None):
        if not group:
            group = '__default'
        val = self._data.get(group, {}).get(k, None)
        return val

    def get_repository(self, aggregator_type):
        return self.get(aggregator_type, 'repository')

    def set_repository(self, repository):
        if callable(repository):
            repository = repository()
        self.set(repository.aggregator_type, repository, 'repository')

    def get_kafka_server(self, name):
        return self.get(name, 'kafka_server')

    def set_kafka_server(self, name, server):
        self.set(name, server, 'kafka_server')


registry = registry()
