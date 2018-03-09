import os
import unittest
import redpipe
import redislite
import redislite.patch

redislite.patch.patch_redis()

from unit_test_setup import hbom, TEST_DIR  # noqa

TEST_DB = os.path.join(TEST_DIR, '.redis.db')
ALT_DB = os.path.join(TEST_DIR, '.redis_alt.db')

default_redis_connection = redislite.StrictRedis(TEST_DB)
alt_redis_connection = redislite.StrictRedis(ALT_DB)

redpipe.connect_redis(default_redis_connection, name='test')
redpipe.connect_redis(alt_redis_connection, name='test_alt')

def clear_redis_testdata():
    default_redis_connection.flushall()
    alt_redis_connection.flushall()


skip_if_redis_disabled = unittest.skipIf(
    default_redis_connection is None, "no redis package installed")
