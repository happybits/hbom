#!/usr/bin/env python

import unittest
from setup import hbom, clear_redis_testdata


class SortedSetModel(hbom.SortedSet):
    pass


class SortedSetTestCase(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_everything(self):
        zorted = SortedSetModel("Person:age")
        zorted.add("1", 29)
        zorted.add("2", 39)
        zorted.add({"3": '15', "4": 35})
        zorted.add({"5": 98, "6": 5})
        self.assertEqual(6, len(zorted))
        self.assertEqual(35, zorted.score("4"))
        self.assertEqual(0, zorted.rank("6"))
        self.assertEqual(5, zorted.revrank("6"))
        self.assertEqual(3, zorted.rank("4"))
        self.assertEqual(["6", "3", "1", "4"], zorted.le(35))

        zorted.add("7", 35)
        self.assertEqual(["4", "7"], zorted.eq(35))
        self.assertEqual(["6", "3", "1"], zorted.lt(30))
        self.assertEqual(["4", "7", "2", "5"], zorted.gt(30))

    def test_delegateable_methods(self):
        zset = SortedSetModel("Person:all")
        zset.zadd("1", 1)
        zset.zadd("2", 2)
        zset.zadd("3", 3)
        zset.zadd("4", 4)
        self.assertEqual(4, zset.zcard())
        self.assertEqual(4, zset.zscore('4'))
        self.assertEqual(['1', '2', '3', '4'], list(zset))
        self.assertEqual(zset.zrange(0, -1), list(zset))
        self.assertEqual(['4', '3', '2', '1'], zset.zrevrange(0, -1))
        self.assertEqual(list(reversed(zset)), zset.zrevrange(0, -1))
        self.assertEqual(list(reversed(zset)), list(zset.__reversed__()))


if __name__ == '__main__':
    unittest.main(verbosity=2)
