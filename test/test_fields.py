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


class TestBooleanField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.BooleanField())

    def test_required(self):
        assert (hbom.BooleanField(required=True))

    def test_default(self):
        assert (hbom.BooleanField(default=True))

    def test_primary(self):
        self.assertRaises(
            hbom.FieldError,
            lambda: hbom.BooleanField(primary=True))


class TestDecimalField(unittest.TestCase):

    def test_noargs(self):
        assert(hbom.DecimalField())

    def test_required(self):
        assert(hbom.DecimalField(required=True))

    def test_default(self):
        assert(hbom.DecimalField(default=1))

    def test_primary(self):
        assert(hbom.DecimalField(primary=True))


class TestFloatField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.FloatField())

    def test_required(self):
        assert (hbom.FloatField(required=True))

    def test_primary(self):
        assert (hbom.FloatField(primary=True))


class TestIntegerField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.IntegerField())

    def test_required(self):
        assert (hbom.IntegerField(required=True))

    def test_default(self):
        assert (hbom.IntegerField(default=1))

    def test_primary(self):
        assert (hbom.IntegerField(primary=True))


class TestStringField(unittest.TestCase):
    def test_noargs(self):
        assert hbom.StringField()

    def test_required(self):
        assert hbom.StringField(required=True)

    def test_default(self):
        assert hbom.StringField(default='a')

    def test_primary(self):
        assert hbom.StringField(primary=True)


class TestTextField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.TextField())

    def test_required(self):
        assert (hbom.TextField(required=True))

    def test_default(self):
        assert (hbom.TextField(default='a'))

    def test_primary(self):
        assert (hbom.TextField(primary=True))


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


class TestStringListField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.StringListField())

    def test_required(self):
        assert (hbom.StringListField(required=True))

    def test_default(self):
        assert (hbom.StringListField(default='a'))

    def test_primary(self):
        self.assertRaises(
            hbom.FieldError, lambda:
            hbom.StringListField(primary=True))

if __name__ == '__main__':
    unittest.main()
