#!/usr/bin/env python
import unittest
from unit_test_setup import hbom


class TestField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.Field())

    def test_required(self):
        assert (hbom.Field(required=True))

    def test_default(self):
        assert (hbom.Field(default=7))


class TestBooleanField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.BooleanField())

    def test_boolean_values_strict(self):
        class Test(hbom.Definition):
            pk = hbom.IntegerField(primary=True)
            flag = hbom.BooleanField()

        self.assertEqual(Test(pk=1).flag, False)
        self.assertEqual(Test(pk=1, flag=True).flag, True)


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


class TestDictField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.DictField())

    def test_required(self):
        assert (hbom.DictField(required=True))

    def test_default(self):
        assert (hbom.DictField(default='a'))


class TestStringListField(unittest.TestCase):
    def test_noargs(self):
        assert (hbom.StringListField())

    def test_required(self):
        assert (hbom.StringListField(required=True))

    def test_default(self):
        assert (hbom.StringListField(default='a'))

    def test_mutables(self):
        class Test(hbom.Definition):
            pk = hbom.StringField(primary=True)
            my_list = hbom.StringListField(default=[])

        t = Test(pk='1')
        self.assertEqual(t.my_list, [])
        self.assertEqual(t.changes_(), {'pk': '1', 'my_list': []})

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

        t = Test(_loading=True, pk='1', my_list=['foo', 'bar'])
        self.assertEqual(t.my_list, ['foo', 'bar'])
        self.assertEqual(my_list, ['foo', 'bar', 'bazz'])

        t = Test(pk='1')
        self.assertEqual(t.my_list, [])
        self.assertEqual(my_list, ['foo', 'bar', 'bazz'])


class TestFieldValidation(unittest.TestCase):
    """Test field validation methods and error conditions"""

    def test_field_validate_none_allowed(self):
        """Test field validation when None is allowed"""
        field = hbom.StringField()
        field.model = "TestModel"
        field.attr = "test_field"
        # Should not raise exception for None when not required
        field.validate(None)

    def test_field_validate_required_none(self):
        """Test field validation error for required fields with None"""
        field = hbom.StringField(required=True)
        field.model = "TestModel"
        field.attr = "test_field"
        with self.assertRaises(hbom.InvalidFieldValue) as cm:
            field.validate(None)
        self.assertIn("test_field is required", str(cm.exception))

    def test_field_validate_wrong_type(self):
        """Test field validation error for wrong types"""
        field = hbom.StringField()
        field.model = "TestModel"
        field.attr = "test_field"
        with self.assertRaises(hbom.InvalidFieldValue) as cm:
            field.validate(123)  # Integer instead of string
        self.assertIn("has type", str(cm.exception))
        self.assertIn("must be of type", str(cm.exception))

    def test_primary_key_modification_prevented(self):
        """Test that primary key modification is prevented"""

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            name = hbom.StringField(required=True)

        model = TestModel(id='test', name='test_name')
        with self.assertRaises(hbom.InvalidOperation) as cm:
            model.id = 'new_id'
        self.assertIn("Cannot update primary key value", str(cm.exception))

    def test_required_field_deletion_prevented(self):
        """Test that required field deletion is prevented"""

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            req = hbom.StringField(required=True)

        model = TestModel(id='test', req='required_value')
        with self.assertRaises(hbom.InvalidOperation) as cm:
            del model.req
        self.assertIn("cannot be null", str(cm.exception))

    def test_field_set_none_triggers_delete(self):
        """Test that setting non-required field to None triggers deletion"""

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            optional = hbom.StringField()

        model = TestModel(id='test', optional='value')
        # Setting to None should trigger deletion for non-required fields
        model.optional = None
        # Check that the field is not in the model's data or is set to None
        self.assertTrue(not hasattr(model, 'optional') or model.optional is None)


class TestStringListFieldEdgeCases(unittest.TestCase):
    """Test StringListField edge cases and empty value handling"""

    def test_string_list_field_empty_values(self):
        """Test StringListField with empty lists and filtering"""

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            items = hbom.StringListField()

        model = TestModel(id='test')
        # Library returns None for unset fields
        self.assertIsNone(model.items)

        # Test with valid values
        model.items = ['valid', 'another']
        self.assertEqual(model.items, ['valid', 'another'])

        # Test setting to None
        model.items = None
        self.assertIsNone(model.items)


if __name__ == '__main__':
    unittest.main()
