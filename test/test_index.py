#!/usr/bin/env python

import unittest
from setup import hbom, clear_redis_testdata


class IndexModel(hbom.Index):
    pass


class HashTestCase(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_basic(self):
        IndexModel.set('foo', 'bar')
        self.assertEqual(IndexModel.get('foo'), 'bar')
        IndexModel.setnx('foo', 'bazz')
        self.assertEqual(IndexModel.get('foo'), 'bar')
        IndexModel.set('foo', 'bazz')
        self.assertEqual(IndexModel.get('foo'), 'bazz')
        IndexModel.remove('foo')
        self.assertEqual(IndexModel.get('foo'), None)
        IndexModel.setnx('foo', 'bazz')
        self.assertEqual(IndexModel.get('foo'), 'bazz')

    def test_multi(self):
        pipe = hbom.Pipeline()
        IndexModel.setnx('foo', 'a', pipe=pipe)
        IndexModel.set('bar', 'b', pipe=pipe)
        IndexModel.setnx('bazz', 'c', pipe=pipe)
        IndexModel.set('bazz', 'd', pipe=pipe)
        IndexModel.setnx('bazz', 'e', pipe=pipe)

        self.assertEqual(IndexModel.mget(['foo', 'bar', 'bazz']), {})
        pipe.execute()
        self.assertEqual(
            IndexModel.mget(['foo', 'bar', 'bazz']),
            {'foo': 'a', 'bar': 'b', 'bazz': 'd'})

        pipe = hbom.Pipeline()
        res = IndexModel.get('foo', pipe=pipe)
        pipe.execute()
        self.assertEqual(res.data, 'a')


if __name__ == "__main__":
    unittest.main(verbosity=2)
