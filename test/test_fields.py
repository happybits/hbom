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

    def test_boolean_values_strict(self):
        class Test(hbom.Definition):
            pk = hbom.IntegerField(primary=True)
            flag = hbom.BooleanField()

        self.assertEqual(Test(pk=1).flag, False)
        self.assertEqual(Test(pk=1, flag=1).flag, True)


class TestDecimalField(unittest.TestCase):

    def test_noargs(self):
        assert(hbom.DecimalField())

    def test_required(self):
        assert(hbom.DecimalField(required=True))

    def test_default(self):
        assert(hbom.DecimalField(default=1))

    def test_primary(self):
        self.assertRaises(TypeError, lambda: hbom.DecimalField(primary=True))

    def test(self):
        class Test(hbom.Definition):
            pk = hbom.IntegerField(primary=True)
            flag = hbom.DecimalField()
        t = Test(pk=1, flag=1.00001)
        self.assertEqual(t.flag, 1.00001)
        t = Test(**t.changes_())
        self.assertEqual(t.flag, 1.00001)


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

    def test_mutables(self):
        class Test(hbom.Definition):
            pk = hbom.StringField(primary=True)
            my_list = hbom.StringListField(default=[])

        t = Test(pk='1')
        self.assertEqual(t.my_list, [])
        self.assertEqual(t.changes_(), {'pk': '1', 'my_list': None})

        my_list = t.my_list
        t.my_list += [
            'foo'
        ]

        t.my_list += [
            'bar'
        ]
        self.assertEqual(t.my_list, ['foo', 'bar'])
        self.assertEqual(my_list, ['foo', 'bar'])

        del t
        my_list += ['bazz']

        self.assertEqual(my_list, ['foo', 'bar', 'bazz'])

        t = Test(_loading=True, pk='1', my_list='foo,bar')
        self.assertEqual(t.my_list, ['foo', 'bar'])
        self.assertEqual(my_list, ['foo', 'bar', 'bazz'])

        t = Test(pk='1')
        self.assertEqual(t.my_list, [])
        self.assertEqual(my_list, ['foo', 'bar', 'bazz'])


if __name__ == '__main__':
    unittest.main()
