#!/usr/bin/env python
from setup import hbom
import unittest
import redis


class TTFoo(hbom.Model):
    pass


class TTBar(hbom.Model):
    _db = redis.StrictRedis(db=14)


class TestConnections(unittest.TestCase):
    def test_connections(self):
        self.assertEqual(TTFoo.db(), hbom.default_connection())
        self.assertNotEqual(TTBar.db(), hbom.default_connection())


if __name__ == '__main__':
    unittest.main()
