#!/usr/bin/env python

import json
from setup import hbom, Toma
import unittest


class OmaModel(Toma):
    a = hbom.IntegerField()
    b = hbom.IntegerField(default=7)
    req = hbom.StringField(required=True)
    j = hbom.JsonField()


class TestModel(unittest.TestCase):
    def test_field_error(self):
        self.assertRaises(hbom.FieldError, OmaModel)

    def test_invalid_field_value(self):
        self.assertRaises(
            hbom.InvalidFieldValue,
            lambda: OmaModel(a='t', req='test'))

    def test_missing_field_value(self):
        self.assertRaises(
            hbom.MissingField,
            lambda: OmaModel(a=1, b=2))

    def test_model_state(self):
        x = OmaModel(a=1, b=2, req='test', id='hello', j=[1, 2])
        self.assertEqual(
            x.to_dict(),
            {'a': 1, 'b': 2, 'id': 'hello', 'j': [1, 2], 'req': 'test'})

    def test_save_new(self):
        x = OmaModel(a=1, b=2, req='test')
        x.save()
        c = x._change_state
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
        x = OmaModel(a=1, b=2, req='test')
        x.save()
        del x.b
        x.a = 3
        x.save()
        c = x._change_state
        self.assertEqual(c['add'], {'a': '3'})
        self.assertEqual(c['remove'], ['b'])
        self.assertEqual(c['changes'], 2)
        self.assertEqual(c['primary_key'], x.primary_key())

    def test_save_w_no_changes(self):
        x = OmaModel(a=1, b=2, req='test')
        x.save()
        x.save()
        c = x._change_state
        self.assertEqual(c['add'], {})
        self.assertEqual(c['remove'], [])
        self.assertEqual(c['changes'], 0)
        self.assertEqual(c['primary_key'], x.primary_key())

    def test_str(self):
        x = OmaModel(a=1, b=2, req='test')
        self.assertEqual(str(x), "<OmaModel:%s>" % x.primary_key())

    def test_repr(self):
        x = OmaModel(a=1, b=2, req='test')
        self.assertEqual(repr(x), json.dumps({'OmaModel': x.to_dict()}))
        print repr(x)


if __name__ == '__main__':
    unittest.main(verbosity=2)
