#!/usr/bin/env python
from setup import hbom, Toma
import unittest


class TTDefault(Toma):
    foo = hbom.ListField(default=[])
    bar = hbom.JsonField(default={})


class TestDefault(unittest.TestCase):
    def test_defaults_not_mutable(self):
        m = TTDefault(foo=[1, 2, 3], bar={'a': 1, 'b': 2})
        self.assertEqual(m.foo, [1, 2, 3])
        self.assertEqual(m.bar, {'a': 1, 'b': 2})

        m = TTDefault()

        self.assertEqual(m.foo, [])
        self.assertEqual(m.bar, {})


if __name__ == '__main__':
    unittest.main(verbosity=2)
