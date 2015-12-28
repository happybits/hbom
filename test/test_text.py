#!/usr/bin/env python
import unittest
from setup import hbom


class TestTextField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.TextField())

    def test_required(self):
        assert (hbom.TextField(required=True))

    def test_default(self):
        assert (hbom.TextField(default='a'))

    def test_primary(self):
        assert (hbom.TextField(primary=True))


if __name__ == '__main__':
    unittest.main(verbosity=2)
