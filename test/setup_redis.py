import os
import redis
import redislite

from setup import hbom, TEST_DIR

try:
    # noinspection PyPackageRequirements
    import rediscluster
except ImportError:
    rediscluster = None


TEST_DB = os.path.join(TEST_DIR, '.redis.db')
TEST_REDIS_CLUSTER = True if os.getenv('TEST_REDIS_CLUSTER', False) else False

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
