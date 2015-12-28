#!/usr/bin/env python
import unittest
from setup import hbom


class TestDecimalField(unittest.TestCase):

    def test_noargs(self):
        assert(hbom.DecimalField())

    def test_required(self):
        assert(hbom.DecimalField(required=True))

    def test_default(self):
        assert(hbom.DecimalField(default=1))

    def test_primary(self):
        assert(hbom.DecimalField(primary=True))


if __name__ == '__main__':
    unittest.main(verbosity=2)
