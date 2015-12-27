import os
import sys
import redis
import redislite
import time

try:
    # noinspection PyPackageRequirements
    import rediscluster
except ImportError:
    rediscluster = None

TEST_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(TEST_DIR)

TEST_DB = os.path.join(TEST_DIR, '.redis.db')


# put our path in front so we can be sure we are testing locally
# not against the global package
sys.path.insert(1, ROOT_DIR)

import hbom  # noqa

TEST_REDIS_CLUSTER = False

if rediscluster and TEST_REDIS_CLUSTER:
    startup_nodes = [{'host': '127.0.0.1', 'port': port}
                     for port in xrange(7000, 7003)]
    r = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes)
else:
    r = redislite.StrictRedis(TEST_DB)

hbom.set_default_redis_connection(r)


def clear_redis_testdata():
    conn = hbom.default_redis_connection()
    if rediscluster and isinstance(conn, rediscluster.StrictRedisCluster):
        conns = [redis.StrictRedis(host=node['host'], port=node['port'])
                 for node in conn.connection_pool.nodes.nodes.values()
                 if node.get('server_type', None) == 'master']
        for conn in conns:
            conn.flushall()
    else:
        conn.flushdb()


class Toma(hbom.BaseModel):
    def _apply_changes(self, old, new, pipe=None):
        response = self._calc_changes(old, new)
        self._change_state = response
        return response['changes']

    @classmethod
    def get_by(cls, **kwargs):
        cls.get_by_kwargs = kwargs
        return set()


class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            print 'elapsed time: %f ms' % self.msecs
