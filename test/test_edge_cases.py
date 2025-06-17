#!/usr/bin/env python

import unittest

from unit_test_setup import hbom, generate_uuid


class TestEdgeCasesAndBoundaryConditions(unittest.TestCase):
    """Test edge cases and boundary conditions for HBOM functionality"""

    def test_unicode_and_special_characters(self):
        """Test handling of unicode and special characters"""

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            text = hbom.StringField()

        # Test unicode characters
        model = TestModel(id='test', text='Hello 世界 🌍')
        self.assertEqual(model.text, 'Hello 世界 🌍')

        # Test special characters
        special_text = "!@#$%^&*()_+-=[]{}|;':\",./<>?"
        model.text = special_text
        self.assertEqual(model.text, special_text)

    def test_very_long_strings(self):
        """Test handling of very long string values"""

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            long_text = hbom.StringField()

        # Test string with 10k characters
        long_string = 'a' * 10000
        model = TestModel(id='test', long_text=long_string)
        self.assertEqual(len(model.long_text), 10000)
        self.assertEqual(model.long_text, long_string)

    def test_empty_string_handling(self):
        """Test handling of empty strings vs None"""

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            optional = hbom.StringField()
            required = hbom.StringField(required=True)

        # Empty string should be allowed for required fields
        model = TestModel(id='test', required='', optional='')
        self.assertEqual(model.required, '')
        self.assertEqual(model.optional, '')

        # But None should not be allowed for required fields
        with self.assertRaises(hbom.MissingField):
            TestModel(id='test', optional='')

    def test_numeric_boundary_values(self):
        """Test numeric fields with boundary values"""

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            integer = hbom.IntegerField()
            float_val = hbom.FloatField()

        # Test zero values
        model = TestModel(id='test', integer=0, float_val=0.0)
        self.assertEqual(model.integer, 0)
        self.assertEqual(model.float_val, 0.0)

        # Test negative values
        model.integer = -999999
        model.float_val = -999.999
        self.assertEqual(model.integer, -999999)
        self.assertEqual(model.float_val, -999.999)

        # Test very large values
        model.integer = 2147483647  # Max 32-bit int
        model.float_val = 1.7976931348623157e+308  # Near max float
        self.assertEqual(model.integer, 2147483647)
        self.assertTrue(model.float_val > 1e+308)

    def test_boolean_edge_cases(self):
        """Test boolean field edge cases"""

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            flag = hbom.BooleanField()

        # Test default value
        model = TestModel(id='test')
        self.assertEqual(model.flag, False)

        # Test explicit True/False
        model.flag = True
        self.assertEqual(model.flag, True)
        model.flag = False
        self.assertEqual(model.flag, False)

    def test_list_field_edge_cases(self):
        """Test list field edge cases"""

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            items = hbom.ListField()

        # Test empty list
        model = TestModel(id='test', items=[])
        self.assertEqual(model.items, [])

        # Test list with None values
        model.items = [1, None, 'test', None, []]
        self.assertEqual(model.items, [1, None, 'test', None, []])

        # Test nested lists
        model.items = [[1, 2], [3, 4], {'a': 'b'}]
        self.assertEqual(len(model.items), 3)

    def test_dict_field_edge_cases(self):
        """Test dict field edge cases"""

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            data = hbom.DictField()

        # Test empty dict
        model = TestModel(id='test', data={})
        self.assertEqual(model.data, {})

        # Test dict with None values
        model.data = {'a': None, 'b': 'value', 'c': None}
        self.assertEqual(model.data['a'], None)
        self.assertEqual(model.data['b'], 'value')

        # Test nested dicts
        model.data = {'level1': {'level2': {'level3': 'deep'}}}
        self.assertEqual(model.data['level1']['level2']['level3'], 'deep')

    def test_field_default_edge_cases(self):
        """Test field default value edge cases"""

        def dynamic_default_func():
            return f"dynamic_{generate_uuid()}"

        class TestModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            static_default = hbom.StringField(default="static")
            dynamic_default = hbom.StringField(default=dynamic_default_func)
            list_default = hbom.ListField(default=[])
            dict_default = hbom.DictField(default={})

        # Test that each instance gets its own default
        model1 = TestModel(id='test1')
        model2 = TestModel(id='test2')

        self.assertEqual(model1.static_default, "static")
        self.assertEqual(model2.static_default, "static")

        # Dynamic defaults should be different
        self.assertNotEqual(model1.dynamic_default, model2.dynamic_default)

        # Mutable defaults should not be shared between instances
        model1.list_default.append('test')
        self.assertEqual(len(model2.list_default), 0)

    def test_model_inheritance_edge_cases(self):
        """Test model inheritance edge cases"""

        class BaseModel(hbom.Definition):
            id = hbom.StringField(primary=True)
            base_field = hbom.StringField()

        class DerivedModel(BaseModel):
            derived_field = hbom.StringField()

        # Test that derived model has both fields
        model = DerivedModel(id='test', base_field='base', derived_field='derived')
        self.assertEqual(model.base_field, 'base')
        self.assertEqual(model.derived_field, 'derived')

        # Test that changes include both field types
        changes = model.changes_()
        self.assertIn('base_field', changes)
        self.assertIn('derived_field', changes)


if __name__ == '__main__':
    unittest.main()
