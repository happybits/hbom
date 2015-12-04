#!/usr/bin/env python

import time
import unittest
from setup import hbom, clear_redis_testdata


class SampleModel(hbom.Model):
    created_at = hbom.FloatField(default=time.time)
    req = hbom.StringField(required=True)


class TestRead(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def initialize(self, ct=1):
        ids = []
        for i in range(0, ct):
            x = SampleModel(u=i, req='test')
            x.save()
            ids.append(x.primary_key())
        return ids

    def test_single_record(self):
        ids = self.initialize(ct=3)
        self.assertEqual(SampleModel.get(ids[0]).id, ids[0])

    def test_multi_record(self):
        ids = self.initialize(ct=5)
        res = [x.id for x in SampleModel.get(ids)]
        self.assertEqual(res, ids)

    def test_missing(self):
        self.assertEqual(SampleModel.get('blah'), None)

    def test_multi_missing(self):
        res = SampleModel.get(['foo', 'bar'])
        self.assertEqual(res, [])

    def test_partial_missing(self):
        ids = self.initialize(ct=5)
        res = [x.id for x in SampleModel.get(['foo'] + ids)]
        self.assertEqual(res, ids)

    def test_multi_by_id_kw(self):
        ids = self.initialize(ct=5)
        res = [x.id for x in SampleModel.get(ids)]
        self.assertEqual(res, ids)


if __name__ == '__main__':
    unittest.main()
