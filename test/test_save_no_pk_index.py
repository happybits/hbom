#!/usr/bin/env python

from setup import hbom, clear_redis_testdata
import unittest


class Model(hbom.RedisModel):
    attr = hbom.StringField()
    _pk_index_enable = False
    _pk_lock_enable = False


class TestSaving(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_save_inline_with_no_attributes(self):
        assert (Model().save())

    def test_save_inline_with_attributes(self):
        assert (Model(attr='hello').save())

    def test_save_with_no_changes(self):
        x = Model()
        assert (x.save())
        self.assertFalse(x.save())
        y = Model.get(x.primary_key())
        self.assertEqual(x.to_dict(), y.to_dict())

    # can specify the id field
    def test_create_w_id(self):
        x = Model(id=1)
        assert (x.save())


if __name__ == '__main__':
    unittest.main(verbosity=2)
