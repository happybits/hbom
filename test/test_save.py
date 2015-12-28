#!/usr/bin/env python

import time
import unittest
from setup import hbom, clear_redis_testdata


class TTSave(hbom.RedisModel):
    a = hbom.IntegerField()
    b = hbom.IntegerField(default=7)
    req = hbom.StringField(required=True)
    created_at = hbom.FloatField(default=time.time)
    _keyspace = 'TT_s'


class TestSave(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_save(self):
        x = TTSave(a=1, b=2, req='test')
        x.save()

    def test_change(self):
        x = TTSave(a=1, b=2, req='test')
        x.save()
        x.b = 4
        assert (x.save())
        y = TTSave.get(x.primary_key())
        self.assertEqual(x.to_dict(), y.to_dict())

    def test_delete(self):
        x = TTSave(a=1, b=2, req='test')
        x.save()
        y = TTSave.get(x.primary_key())
        y.delete()
        assert (TTSave.get(x.primary_key()) is None)


if __name__ == '__main__':
    unittest.main(verbosity=2)
