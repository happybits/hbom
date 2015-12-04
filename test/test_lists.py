#!/usr/bin/env python

import unittest
from setup import hbom, clear_redis_testdata


class ListModel(hbom.List):
    pass


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


if __name__ == '__main__':
    unittest.main()
