#!/usr/bin/env python
import unittest
from setup import hbom


class TestIntegerField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.IntegerField())

    def test_required(self):
        assert (hbom.IntegerField(required=True))

    def test_unique(self):
        assert (hbom.IntegerField(unique=True))

    def test_default(self):
        assert (hbom.IntegerField(default=1))

    def test_primary(self):
        assert (hbom.IntegerField(primary=True))


if __name__ == '__main__':
    unittest.main()
