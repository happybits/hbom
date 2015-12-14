#!/usr/bin/env python
import unittest
import redislite
from setup import hbom, TEST_DIR
import os


class TTFoo(hbom.RedisModel):
    pass


class TTBar(hbom.RedisModel):
    _db = redislite.StrictRedis(os.path.join(TEST_DIR, '.redis_alt.db'))


class TestConnections(unittest.TestCase):
    def test_connections(self):
        self.assertEqual(TTFoo.db(), hbom.default_redis_connection())
        self.assertNotEqual(TTBar.db(), hbom.default_redis_connection())


if __name__ == '__main__':
    unittest.main()
