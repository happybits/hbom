import os
import sys
import redis

try:
    # noinspection PyPackageRequirements
    import rediscluster
except ImportError:
    rediscluster = None

# put our path in front so we can be sure we are testing locally
# not against the global package
sys.path.insert(1, os.path.dirname(os.path.dirname(__file__)))

# noinspection PyPep8
import hbom
# noinspection PyPep8
import hbom.model

# noinspection PyPep8

TEST_REDIS_CLUSTER = False

if rediscluster and TEST_REDIS_CLUSTER:
    startup_nodes = [{'host': '127.0.0.1', 'port': port}
                     for port in xrange(7000, 7003)]
    r = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes)
else:
    r = redis.StrictRedis(db=15)

hbom.set_default_connection(r)


def clear_redis_testdata():
    conn = hbom.default_connection()
    if rediscluster and isinstance(conn, rediscluster.StrictRedisCluster):
        conns = [redis.StrictRedis(host=node['host'], port=node['port'])
                 for node in conn.connection_pool.nodes.nodes.values()
                 if node.get('server_type', None) == 'master']
        for conn in conns:
            conn.flushall()
    else:
        conn.flushdb()


class Toma(hbom.model.Oma):
    def _apply_changes(self, old, new, pipe=None):
        response = self._calc_changes(old, new)
        self._change_state = response
        return response['changes']

    @classmethod
    def get_by(cls, **kwargs):
        cls.get_by_kwargs = kwargs
        return set()
