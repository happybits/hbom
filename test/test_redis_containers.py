#!/usr/bin/env python

# std-lib
import unittest

# test-harness
from setup_redis import (
    hbom,
    clear_redis_testdata,
    default_redis_connection,
    skip_if_redis_disabled,
)


class ListModel(hbom.RedisList):
    _db = default_redis_connection


@skip_if_redis_disabled
class ListTestCase(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_common_operations(self):
        alpha = ListModel('alpha')

        # append
        alpha.append('a')
        alpha.append('b')
        alpha.append('c', 'd')
        alpha.append(['e', 'f'])

        self.assertEqual(['a', 'b', 'c', 'd', 'e', 'f'], alpha.all())

        # len
        self.assertEqual(6, len(alpha))

        num = ListModel('num')
        num.append('1')
        num.append('2')

        # extend and iter
        alpha.extend(num)
        self.assertEqual(
            ['a', 'b', 'c', 'd', 'e', 'f', '1', '2'],
            alpha.all())
        alpha.extend(['3', '4'])
        self.assertEqual(
            ['a', 'b', 'c', 'd', 'e', 'f', '1', '2', '3', '4'],
            alpha.all())

        # contains
        self.assertTrue('b' in alpha)
        self.assertTrue('2' in alpha)
        self.assertTrue('5' not in alpha)

        # shift and unshift
        num.unshift('0')
        self.assertEqual(['0', '1', '2'], num.members)
        self.assertEqual('0', num.shift())
        self.assertEqual(['1', '2'], num.members)

        # push and pop
        num.push('4')
        num.push('a', 'b')
        num.push(['c', 'd'])
        self.assertEqual('d', num.pop())
        self.assertEqual('c', num.pop())
        self.assertEqual(['1', '2', '4', 'a', 'b'], num.members)

        # trim
        alpha.trim(0, 1)
        self.assertEqual(['a', 'b'], alpha.all())

        # remove
        alpha.remove('b')
        self.assertEqual(['a'], alpha.all())

        # setitem
        alpha[0] = 'A'
        self.assertEqual(['A'], alpha.all())

        # iter
        alpha.push('B')
        for e, a in zip(alpha, ['A', 'B']):
            self.assertEqual(a, e)
        self.assertEqual(['A', 'B'], list(alpha))

        # slice
        alpha.extend(['C', 'D', 'E'])
        self.assertEqual(['A', 'B', 'C', 'D', 'E'], alpha[:])
        self.assertEqual(['B', 'C'], alpha[1:3])

        alpha.reverse()
        self.assertEqual(['E', 'D', 'C', 'B', 'A'], list(alpha))

    def test_pop_onto(self):
        a = ListModel('alpha')
        b = ListModel('beta')
        a.extend(range(10))

        # test pop_onto
        a_snap = list(a.members)
        while True:
            v = a.pop_onto(b.key)
            if not v:
                break
            else:
                self.assertTrue(v not in a.members)
                self.assertTrue(v in b.members)

        self.assertEqual(a_snap, b.members)

        # test rpoplpush
        b_snap = list(b.members)
        while True:
            v = b.rpoplpush(a.key)
            if not v:
                break
            else:
                self.assertTrue(v in a.members)
                self.assertTrue(v not in b.members)

        self.assertEqual(b_snap, a.members)

    def test_native_methods(self):
        l = ListModel('mylist')
        self.assertEqual([], l.lrange(0, -1))
        l.rpush('b')
        l.rpush('c')
        l.lpush('a')
        self.assertEqual(['a', 'b', 'c'], l.lrange(0, -1))
        self.assertEqual(3, l.llen())
        l.ltrim(1, 2)
        self.assertEqual(['b', 'c'], l.lrange(0, -1))
        self.assertEqual('c', l.lindex(1))
        self.assertEqual(1, l.lset(0, 'a'))
        self.assertEqual(1, l.lset(1, 'b'))
        self.assertEqual(['a', 'b'], l.lrange(0, -1))
        self.assertEqual('a', l.lpop())
        self.assertEqual('b', l.rpop())


class SampleSet(hbom.RedisSet):
    _db = default_redis_connection


@skip_if_redis_disabled
class SetTestCase(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_common_operations(self):
        fruits = SampleSet(key='fruits')
        fruits.add('apples')
        fruits.add('oranges')
        fruits.add('bananas', 'tomatoes')
        fruits.add(['strawberries', 'blackberries'])

        self.assertEqual(
            {'apples', 'oranges', 'bananas',
             'tomatoes', 'strawberries', 'blackberries'}, fruits.all())

        # remove
        fruits.remove('apples')
        fruits.remove('bananas', 'blackberries')
        fruits.remove(['tomatoes', 'strawberries'])

        self.assertEqual({'oranges'}, fruits.all())

        # in
        self.assertTrue('oranges' in fruits)
        self.assertTrue('apples' not in fruits)

        # len
        self.assertEqual(1, len(fruits))

        # pop
        self.assertEqual('oranges', fruits.pop())

    def test_access_redis_methods(self):
        s = SampleSet('new_set')
        s.sadd('a')
        s.sadd('b')
        s.srem('b')
        self.assertEqual('a', s.spop())
        s.sadd('a')
        self.assert_('a' in s.members)
        s.sadd('b')
        self.assertEqual(2, s.scard())
        self.assert_(s.sismember('a'))
        conn = default_redis_connection
        conn.sadd('other_set', 'a')
        conn.sadd('other_set', 'b')
        conn.sadd('other_set', 'c')
        self.assert_(s.srandmember() in {'a', 'b'})


class SortedSetModel(hbom.RedisSortedSet):
    _db = default_redis_connection


@skip_if_redis_disabled
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


class HashModel(hbom.RedisHash):
    _db = default_redis_connection


@skip_if_redis_disabled
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

        pulled = default_redis_connection.hgetall(h.key)
        self.assertEqual({'name': "Richard Cypher",
                          'real_name': "Richard Rahl"}, pulled)

        self.assertEqual(['name', 'real_name'], h.hkeys())
        self.assertEqual(["Richard Cypher", "Richard Rahl"],
                         h.hvals())

        del h['name']
        pulled = default_redis_connection.hgetall(h.key)
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


class IndexModel(hbom.RedisIndex):
    _db = default_redis_connection


@skip_if_redis_disabled
class IndexTestCase(unittest.TestCase):
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
        res = IndexModel.mget(['foo', 'bar', 'bazz'], pipe=pipe)
        pipe.execute()
        self.assertEqual(
            {k: v.data for k, v in res.items()},
            {'foo': 'a', 'bar': 'b', 'bazz': 'd'})

        pipe = hbom.Pipeline()
        res = IndexModel.get('foo', pipe=pipe)
        pipe.execute()
        self.assertEqual(res.data, 'a')


class SortedSetDemo(hbom.redis_backend.RedisSortedSet):
    _keyspace = 'TT_SortedidTest'
    _db = default_redis_connection


@skip_if_redis_disabled
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