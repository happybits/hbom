#!/usr/bin/env python
import unittest
from setup import hbom


class TestBooleanField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.BooleanField())

    def test_required(self):
        assert (hbom.BooleanField(required=True))

    def test_unique(self):
        self.assertRaises(
            hbom.FieldError,
            lambda: hbom.BooleanField(unique=True))

    def test_default(self):
        assert (hbom.BooleanField(default=True))

    def test_primary(self):
        self.assertRaises(
            hbom.FieldError,
            lambda: hbom.BooleanField(primary=True))


if __name__ == '__main__':
    unittest.main()
