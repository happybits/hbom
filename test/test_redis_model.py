#!/usr/bin/env python

# std lib
import time
import unittest
import os
from uuid import uuid4

# 3rd-party
import redislite

# test harness
from setup import generate_uuid
from setup_redis import hbom, clear_redis_testdata, TEST_DIR


class TTSave(hbom.RedisModel):
    id = hbom.StringField(primary=True, default=generate_uuid)
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


class LightModel(hbom.RedisModel):
    id = hbom.StringField(primary=True, default=generate_uuid)
    attr = hbom.StringField()


class TestLightModel(unittest.TestCase):
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


class PkModel(hbom.RedisModel):
    myid = hbom.StringField(primary=True, default=lambda: str(uuid4()))


class TestPK(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_save(self):
        x = PkModel()
        assert x.save()
        assert x.myid
        y = PkModel.get(x.myid)
        self.assertEqual(x.to_dict(), y.to_dict())

    def test_save_pipe(self):
        x = PkModel()
        pipe = hbom.Pipeline()
        x.save(pipe=pipe)
        self.assertEqual(PkModel.get(x.primary_key()), None)
        pipe.execute()
        self.assertEqual(PkModel.get(x.primary_key()).primary_key(),
                         x.primary_key())


class Demo(hbom.RedisModel):
    id = hbom.StringField(primary=True)
    _keyspace = 'TT_idTest'


class TestModelIds(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_ids(self):
        expected_ids = []
        for i in xrange(1, 201):
            x = Demo(id='a-%s' % i)
            x.save()
            expected_ids.append(x.id)

        ids = set([i for i in Demo.ids()])
        self.assertEqual(ids, set(expected_ids))


class SampleModel(hbom.RedisModel):
    id = hbom.StringField(primary=True, default=generate_uuid)
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


class TTFoo(hbom.RedisModel):
    id = hbom.StringField(primary=True, default=generate_uuid)
    pass


class TTBar(hbom.RedisModel):
    id = hbom.StringField(primary=True, default=generate_uuid)
    _db = redislite.StrictRedis(os.path.join(TEST_DIR, '.redis_alt.db'))


class TestConnections(unittest.TestCase):
    def test_connections(self):
        self.assertEqual(TTFoo.db(), hbom.default_redis_connection())
        self.assertNotEqual(TTBar.db(), hbom.default_redis_connection())


if __name__ == '__main__':
    unittest.main(verbosity=2)
