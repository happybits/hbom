#!/usr/bin/env python

import json
from setup import hbom, StubModel, StubModelChanges
import unittest


class SampleModel(StubModel):
    a = hbom.IntegerField()
    b = hbom.IntegerField(default=7)
    req = hbom.StringField(required=True)
    j = hbom.JsonField()


class TestModel(unittest.TestCase):
    def test_field_error(self):
        self.assertRaises(hbom.FieldError, SampleModel)

    def test_invalid_field_value(self):
        self.assertRaises(
            hbom.InvalidFieldValue,
            lambda: SampleModel(a='t', req='test'))

    def test_missing_field_value(self):
        self.assertRaises(
            hbom.MissingField,
            lambda: SampleModel(a=1, b=2))

    def test_model_state(self):
        x = SampleModel(a=1, b=2, req='test', id='hello', j=[1, 2])
        self.assertEqual(
            x.to_dict(),
            {'a': 1, 'b': 2, 'id': 'hello', 'j': [1, 2], 'req': 'test'})

    def test_save_new(self):
        del StubModelChanges[:]
        x = SampleModel(a=1, b=2, req='test')
        x.save()
        c = StubModelChanges.pop()
        assert (isinstance(c['primary_key'], str))

        expected = x.to_dict()
        expected.pop('j')

        self.assertEqual(set(c['add'].keys()), set(expected.keys()))
        for attr in expected:
            self.assertEqual(c['add'][attr], str(expected[attr]))

        self.assertEqual(c['remove'], [])
        self.assertEqual(c['changes'], 4)
        self.assertEqual(c['primary_key'], x.primary_key())

    def test_save_w_changes(self):
        x = SampleModel(a=1, b=2, req='test')
        x.save()
        del x.b
        x.a = 3
        del StubModelChanges[:]
        x.save()
        c = StubModelChanges.pop()
        self.assertEqual(c['add'], {'a': '3'})
        self.assertEqual(c['remove'], ['b'])
        self.assertEqual(c['changes'], 2)
        self.assertEqual(c['primary_key'], x.primary_key())

    def test_save_w_no_changes(self):
        x = SampleModel(a=1, b=2, req='test')
        x.save()
        del StubModelChanges[:]
        x.save()
        c = StubModelChanges.pop()
        self.assertEqual(c['add'], {})
        self.assertEqual(c['remove'], [])
        self.assertEqual(c['changes'], 0)
        self.assertEqual(c['primary_key'], x.primary_key())

    def test_str(self):
        x = SampleModel(a=1, b=2, req='test')
        self.assertEqual(str(x), "<SampleModel:%s>" % x.primary_key())

    def test_repr(self):
        x = SampleModel(a=1, b=2, req='test')
        self.assertEqual(json.loads(repr(x)), {'SampleModel': x.to_dict()})


class TTDefault(StubModel):
    foo = hbom.ListField(default=[])
    bar = hbom.JsonField(default={})


class TestDefault(unittest.TestCase):
    def test_defaults_not_mutable(self):
        m = TTDefault(foo=[1, 2, 3], bar={'a': 1, 'b': 2})
        self.assertEqual(m.foo, [1, 2, 3])
        self.assertEqual(m.bar, {'a': 1, 'b': 2})

        m = TTDefault()

        self.assertEqual(m.foo, [])
        self.assertEqual(m.bar, {})


if __name__ == '__main__':
    unittest.main(verbosity=2)
