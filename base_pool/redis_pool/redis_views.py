from redis import ConnectionPool, Redis

from conf.common import *


class RedisClient(object):
    __pool = None

    def __init__(self):
        host = REDIS_HOST
        port = REDIS_PORT
        max_connections = 10

        if not self.__pool:
            self.__class__.__pool = ConnectionPool(host=host, port=port, max_connections=max_connections)

    def __get_connect(self):
        self._conn = Redis(connection_pool=self.__pool, decode_responses=True)

    def select_data(self, key):
        """
        查询数据
        :param key:
        :return:
        """
        data = self._conn.get(key).decode('utf-8')
        return data


redis_conn = RedisClient()
