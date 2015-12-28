#!/usr/bin/env python
import unittest
from setup import hbom


class TestField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.Field())

    def test_required(self):
        assert (hbom.Field(required=True))

    def test_default(self):
        assert (hbom.Field(default=7))

    def test_primary(self):
        self.assertRaises(
            hbom.FieldError,
            lambda: hbom.Field(primary=True))


if __name__ == '__main__':
    unittest.main()
