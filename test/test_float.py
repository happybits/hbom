#!/usr/bin/env python
import unittest
from setup import hbom


class TestFloatField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.FloatField())

    def test_required(self):
        assert (hbom.FloatField(required=True))

    def test_primary(self):
        assert (hbom.FloatField(primary=True))


if __name__ == '__main__':
    unittest.main()
