#!/usr/bin/env python

import unittest
from setup import hbom, clear_redis_testdata


class SampleSet(hbom.Set):
    pass


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
        conn = hbom.default_connection()
        conn.sadd('other_set', 'a')
        conn.sadd('other_set', 'b')
        conn.sadd('other_set', 'c')
        self.assert_(s.srandmember() in {'a', 'b'})


if __name__ == '__main__':
    unittest.main()
