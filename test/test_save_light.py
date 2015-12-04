#!/usr/bin/env python

from setup import hbom, clear_redis_testdata
import unittest


class LightModel(hbom.RedisModel):
    attr = hbom.StringField()


class TestSaving(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_save_inline_with_no_attributes(self):
        assert (LightModel().save())

    def test_save_inline_with_attributes(self):
        assert (LightModel(attr='hello').save())

    def test_save_with_no_changes(self):
        x = LightModel()
        assert (x.save())
        self.assertFalse(x.save())
        y = LightModel.get(x.primary_key())
        self.assertEqual(x.to_dict(), y.to_dict())


if __name__ == '__main__':
    unittest.main(verbosity=2)
