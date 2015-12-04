#!/usr/bin/env python
import unittest
from setup import hbom, clear_redis_testdata


class Demo(hbom.RedisModel):
    id = hbom.StringField(primary=True)
    _keyspace = 'TT_idTest'


class SortedSetDemo(hbom.redis_backend.RedisSortedSet):
    _keyspace = 'TT_SortedidTest'


class TestModelIds(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_ids(self):
        expected_ids = []
        for i in xrange(1, 201):
            x = Demo(id='a-%s' % i)
            x.save()
            expected_ids.append(x.id)

        ids = set([i for i in Demo.ids()])
        self.assertEqual(ids, set(expected_ids))


class TestSortedSetIds(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_ids(self):
        expected_ids = []
        for x in xrange(1, 201):
            i = 'a-%s' % x
            SortedSetDemo(i).add('test', 1)
            expected_ids.append(i)

        ids = set([x for x in SortedSetDemo.ids()])
        self.assertEqual(ids, set(expected_ids))


if __name__ == '__main__':
    unittest.main(verbosity=2)
