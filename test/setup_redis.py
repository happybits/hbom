import os
import unittest

try:
    import redis
except ImportError:
    redis = None

try:
    import redislite
    import redislite.patch
except ImportError:
    redislite = None

from setup import hbom, TEST_DIR  # noqa

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
    default_redis_connection = rediscluster.StrictRedisCluster(
        startup_nodes=startup_nodes)

elif redislite:
    default_redis_connection = redislite.StrictRedis(TEST_DB)
    redislite.patch.patch_redis()

elif redis:
    default_redis_connection = redis.StrictRedis()

else:
    default_redis_connection = None


def clear_redis_testdata():
    conn = default_redis_connection
    if conn is None:
        return

    if rediscluster and isinstance(conn, rediscluster.StrictRedisCluster):
        conns = [redis.StrictRedis(host=node['host'], port=node['port'])
                 for node in conn.connection_pool.nodes.nodes.values()
                 if node.get('server_type', None) == 'master']
        for conn in conns:
            conn.flushall()
    else:
        conn.flushdb()


skip_if_redis_disabled = unittest.skipIf(
    default_redis_connection is None, "no redis package installed")
