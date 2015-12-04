#!/usr/bin/env python

import unittest
from setup import hbom, clear_redis_testdata


class HashModel(hbom.Hash):
    pass


class HashTestCase(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_basic(self):
        h = HashModel('hkey')
        self.assertEqual(0, len(h))
        h['name'] = "Richard Cypher"
        h['real_name'] = "Richard Rahl"

        pulled = hbom.default_connection().hgetall(h.key)
        self.assertEqual({'name': "Richard Cypher",
                          'real_name': "Richard Rahl"}, pulled)

        self.assertEqual(['name', 'real_name'], h.hkeys())
        self.assertEqual(["Richard Cypher", "Richard Rahl"],
                         h.hvals())

        del h['name']
        pulled = hbom.default_connection().hgetall(h.key)
        self.assertEqual({'real_name': "Richard Rahl"}, pulled)
        self.assert_('real_name' in h)
        h.dict = {"new_hash": "YEY"}
        self.assertEqual({"new_hash": "YEY"}, h.dict)

    def test_delegateable_methods(self):
        h = HashModel('my_hash')
        h.hincrby('Red', 1)
        h.hincrby('Red', 1)
        h.hincrby('Red', 2)
        self.assertEqual(4, int(h.hget('Red')))
        h.hmset({'Blue': 100, 'Green': 19, 'Yellow': 1024})
        self.assertEqual(['100', '19'], h.hmget(['Blue', 'Green']))


if __name__ == "__main__":
    import sys

    unittest.main(argv=sys.argv)
