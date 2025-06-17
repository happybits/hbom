#!/usr/bin/env python
# -*- coding: utf-8 -*-
# std-lib
from builtins import range
import time
import unittest

# test harness
from unit_test_setup import generate_uuid

# test-harness
from setup_redis import (
    hbom,
    clear_redis_testdata,
    skip_if_redis_disabled,
)


class TTSave(hbom.RedisObject):
    class definition(hbom.Definition):
        id = hbom.StringField(primary=True, default=generate_uuid)
        a = hbom.IntegerField()
        b = hbom.IntegerField(default=7)
        req = hbom.TextField(required=True)
        created_at = hbom.FloatField(default=time.time)

    _keyspace = 'TT_s'
    _db = 'test'


@skip_if_redis_disabled
class TestSave(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_save(self):
        x = TTSave.new(a=1, b=2, req=u'💡')
        TTSave.save(x)
        y = TTSave.get(x.id)
        self.assertEqual(y.req, u'💡')

    def test_change(self):
        x = TTSave.new(a=1, b=2, req='test')
        TTSave.save(x)
        x.b = 4
        assert (TTSave.save(x))
        y = TTSave.get(x.primary_key())
        self.assertEqual(dict(x), dict(y))

    def test_delete(self):
        x = TTSave.new(a=1, b=2, req='test')
        TTSave.save(x)
        TTSave.delete(x.primary_key())
        self.assertFalse(TTSave.get(x.primary_key()).exists())


class Demo(hbom.RedisObject):
    class definition(hbom.Definition):
        id = hbom.StringField(primary=True)

    _keyspace = 'TT_idTest'
    _db = 'test'


@skip_if_redis_disabled
class TestModelIds(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_ids(self):
        expected_ids = []
        for i in range(1, 201):
            x = Demo.new(id='a-%s' % i)
            Demo.save(x)
            expected_ids.append(x.id)

        ids = set([i for i in Demo.storage.ids()])
        self.assertEqual(ids, set(expected_ids))


class SampleModel(hbom.RedisObject):
    class definition(hbom.Definition):
        id = hbom.StringField(primary=True, default=generate_uuid)
        created_at = hbom.FloatField(default=time.time)
        req = hbom.StringField(required=True)

    _db = 'test'
    _keyspace = 'SampleModel'


@skip_if_redis_disabled
class TestRead(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def initialize(self, ct=1):
        ids = []
        for i in range(0, ct):
            x = SampleModel.new(u=i, req='test')
            SampleModel.save(x)
            ids.append(x.primary_key())
        return ids

    def test_single_record(self):
        ids = self.initialize(ct=3)
        self.assertEqual(SampleModel.get(ids[0]).id, ids[0])

    def test_multi_record(self):
        ids = self.initialize(ct=5)
        res = [x.id for x in SampleModel.get_multi(ids)]
        self.assertEqual(res, ids)

    def test_missing(self):
        self.assertEqual(SampleModel.get('blah').exists(), False)

    def test_multi_missing(self):
        res = [m for m in SampleModel.get_multi(['foo', 'bar']) if m.exists()]
        self.assertEqual(res, [])

    def test_partial_missing(self):
        ids = self.initialize(ct=5)
        res = [x.id for x in SampleModel.get_multi(['foo'] + ids) if x.exists()]
        self.assertEqual(res, ids)

    def test_multi_by_id_kw(self):
        ids = self.initialize(ct=5)
        res = [x.id for x in SampleModel.get_multi(ids) if x.exists()]
        self.assertEqual(res, ids)


class SLModel(hbom.RedisObject):
    class definition(hbom.Definition):
        id = hbom.StringField(primary=True, default=generate_uuid)
        data = hbom.StringListField(default=lambda: [])

    _db = 'test'
    _keyspace = 'SLModel'


class SLModelE(SLModel):
    _keyspace = 'SLModelE'


class SLModelEU(SLModelE):
    _db = 'foo'


class StringListModelTestCase(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test(self):
        m = SLModel.new()
        SLModel.save(m)

        m = SLModel.get(m.primary_key())
        self.assertEqual(m.data, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
