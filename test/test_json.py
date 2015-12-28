#!/usr/bin/env python
import unittest
from setup import hbom


class TestJsonField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.JsonField())

    def test_required(self):
        assert (hbom.JsonField(required=True))

    def test_default(self):
        assert (hbom.JsonField(default='a'))

    def test_primary(self):
        self.assertRaises(
            hbom.FieldError, lambda:
            hbom.JsonField(primary=True))


if __name__ == '__main__':
    unittest.main()
