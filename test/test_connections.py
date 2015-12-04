#!/usr/bin/env python
import unittest

import redis
from setup import hbom


class TTFoo(hbom.RedisModel):
    pass


class TTBar(hbom.RedisModel):
    _db = redis.StrictRedis(db=14)


class TestConnections(unittest.TestCase):
    def test_connections(self):
        self.assertEqual(TTFoo.db(), hbom.default_redis_connection())
        self.assertNotEqual(TTBar.db(), hbom.default_redis_connection())


if __name__ == '__main__':
    unittest.main()
