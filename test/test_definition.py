#!/usr/bin/env python

import json
from unit_test_setup import hbom, generate_uuid
import unittest


class SampleModel(hbom.Definition):
    id = hbom.StringField(primary=True, default=generate_uuid)
    a = hbom.IntegerField()
    b = hbom.IntegerField(default=7)
    req = hbom.StringField(required=True)
    j = hbom.DictField()


class TestModel(unittest.TestCase):
    def test_field_error(self):
        self.assertRaises(hbom.FieldError, SampleModel)

    def test_multiple_primary_keys_error(self):
        """Test that defining multiple primary key fields raises FieldError"""
        with self.assertRaises(hbom.FieldError) as cm:
            class BadModel(hbom.Definition):
                id1 = hbom.StringField(primary=True)
                id2 = hbom.StringField(primary=True)
        self.assertIn("One primary field allowed", str(cm.exception))

    def test_no_primary_key_error(self):
        """Test that defining no primary key field raises FieldError"""
        with self.assertRaises(hbom.FieldError) as cm:
            class NoPrimaryModel(hbom.Definition):
                name = hbom.StringField()
                value = hbom.IntegerField()
        self.assertIn("No primary field specified", str(cm.exception))

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
        expected = {'a': 1, 'b': 2, 'id': 'hello', 'j': [1, 2], 'req': 'test'}
        self.assertEqual(x.__dict__, expected)
        self.assertEqual(dict(x), expected)

    def test_changes(self):
        x = SampleModel(a=1, b=2, req='test')
        c = x.changes_()
        add = {k: v for k, v in c.items() if v is not None}
        remove = [k for k, v in c.items() if v is None]

        expected = dict(x)
        expected.pop('j')

        self.assertEqual(set(add.keys()), set(expected.keys()))
        for attr in expected:
            self.assertEqual(add[attr], expected[attr])

        self.assertEqual(remove, [])
        self.assertEqual(len(c), 4)

    def test_save_w_changes(self):
        x = SampleModel(a=1, b=2, req='test')
        x.persisted_()
        del x.b
        x.a = 3
        c = x.changes_()
        add = {k: v for k, v in c.items() if v is not None}
        remove = [k for k, v in c.items() if v is None]

        self.assertEqual(add, {'a': 3})
        self.assertEqual(remove, ['b'])
        self.assertEqual(len(c), 2)

    def test_save_w_no_changes(self):
        x = SampleModel(a=1, b=2, req='test')
        x.persisted_()
        c = x.changes_()
        add = {k: v for k, v in c.items() if v is not None}
        remove = [k for k, v in c.items() if v is None]

        self.assertEqual(add, {})
        self.assertEqual(remove, [])
        self.assertEqual(len(c), 0)

    def test_str(self):
        x = SampleModel(a=1, b=2, req='test')
        self.assertEqual(str(x), "<SampleModel:%s>" % x.primary_key())

    def test_repr(self):
        x = SampleModel(a=1, b=2, req='test')
        self.assertEqual(json.loads(repr(x)), {'SampleModel': dict(x)})

    def test_load_all_none_values(self):
        """Test load_ method when all data values are None"""
        model = SampleModel(req='test')
        # This should handle gracefully when all values are None
        model.load_([None, None, None, None])
        # Should maintain existing values since all loaded values are None
        self.assertEqual(model.req, 'test')

    def test_attach_without_parent(self):
        """Test attach method when _parent is None"""
        model = SampleModel(req='test')
        model._parent = None
        # Should not raise exception when no parent is set - requires pipe argument
        pipe = hbom.Pipeline()
        model.attach(pipe)

    def test_changes_without_primary_key(self):
        """Test changes_ method when primary key is not set"""
        # Create model without setting primary key
        model = SampleModel.__new__(SampleModel)
        model._data = {}  # Initialize _data dict
        model._dirty = set()  # Initialize _dirty set
        model.req = 'test'
        model.a = 1
        # Remove primary key from _data to simulate missing primary key
        if 'id' in model._data:
            del model._data['id']
        # Should raise FieldError when primary key is missing
        with self.assertRaises(hbom.FieldError) as cm:
            model.changes_()
        self.assertIn("Missing primary key value", str(cm.exception))


class TTDefault(hbom.Definition):
    id = hbom.StringField(primary=True, default=generate_uuid)
    foo = hbom.ListField(default=[])
    bar = hbom.DictField(default={})


class TestDefault(unittest.TestCase):
    def test_defaults_not_mutable(self):
        m = TTDefault(foo=[1, 2, 3], bar={'a': 1, 'b': 2})
        self.assertEqual(m.foo, [1, 2, 3])
        self.assertEqual(m.bar, {'a': 1, 'b': 2})

        m = TTDefault()

        self.assertEqual(m.foo, [])
        self.assertEqual(m.bar, {})


class TestModelWithStringListField(unittest.TestCase):

    @property
    def sample(self):
        class Sample(hbom.Definition):
            id = hbom.StringField(primary=True, default=generate_uuid)
            foo = hbom.StringListField()

        return Sample

    def test_with_array(self):
        sample = self.sample(foo=['test', 'moo'])
        self.assertEqual(sample.foo, ['test', 'moo'])

    def test_with_empty(self):
        sample = self.sample(foo=None)
        self.assertEqual(sample.foo, None)

    def test_with_none(self):
        sample = self.sample(foo=None)
        self.assertEqual(sample.foo, None)


class TestModelWithRequiredStringListField(unittest.TestCase):

    @property
    def sample(self):
        class Sample(hbom.Definition):
            id = hbom.StringField(primary=True, default=generate_uuid)
            foo = hbom.StringListField(required=True)

        return Sample

    def test_with_array(self):
        sample = self.sample(foo=['test'])
        self.assertEqual(sample.foo, ['test'])

    def test_with_empty_string(self):
        self.assertRaises(hbom.InvalidFieldValue, lambda: self.sample(foo=''))

    def test_with_none(self):
        self.assertRaises(hbom.MissingField, lambda: self.sample(foo=None))


if __name__ == '__main__':
    unittest.main(verbosity=2)
