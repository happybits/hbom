#!/usr/bin/env python
import unittest
from setup import hbom


class TestStringField(unittest.TestCase):
    def test_noargs(self):
        assert hbom.StringField()

    def test_required(self):
        assert hbom.StringField(required=True)

    def test_default(self):
        assert hbom.StringField(default='a')

    def test_primary(self):
        assert hbom.StringField(primary=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
