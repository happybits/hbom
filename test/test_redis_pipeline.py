#!/usr/bin/env python

# std lib
import time
import unittest
from uuid import uuid4
import os

# 3rd-party
import redis
import redislite

# test-harness
from setup import generate_uuid
from setup_redis import hbom, clear_redis_testdata, TEST_DIR


class Foo(hbom.RedisModel):
    id = hbom.StringField(primary=True, default=generate_uuid)
    a = hbom.StringField(required=True)
    _keyspace = 'TT_foo'


class Bar(hbom.RedisSortedSet):
    _keyspace = 'TT_bar'
    a = hbom.StringField()


class Bazz(hbom.RedisModel):
    _keyspace = 'TT_bazz'
    id = hbom.StringField(primary=True, default=generate_uuid)
    a = hbom.StringField()


class Quux(hbom.RedisModel):
    _keyspace = 'TT_quux'
    id = hbom.StringField(primary=True, default=generate_uuid)
    a = hbom.StringField()
    _db = redislite.StrictRedis(os.path.join(TEST_DIR, '.redis_pipe.db'))


class ErrorModel(hbom.RedisModel):
    _keyspace = 'TT_err'
    id = hbom.StringField(primary=True, default=generate_uuid)
    a = hbom.StringField()
    _db = redis.StrictRedis(db=14, port=3322191)


class Sample(hbom.RedisModel):
    a = hbom.StringField(primary=True, required=True)
    b = hbom.IntegerField()
    _keyspace = 'TT_sample'


class TestPipeline(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_pipeline_model(self):
        pipe = hbom.Pipeline()
        i = 'abc123'
        now = int(time.time())
        s = Sample(a=i)
        s.b = now
        s.save(pipe=pipe)
        r = Sample.ref(i, pipe=pipe)
        self.assertIsNone(Sample.get(i))
        self.assertIsNone(r.b)
        pipe.execute()
        self.assertEqual(Sample.get(i).b, now)
        self.assertEqual(r.b, now)

    def test_pipeline_container(self):
        pipe = hbom.Pipeline()
        i = 1
        ref = Bar(i, pipe=pipe)
        now = time.time()
        write_response = ref.zadd('a', now)
        read_response = ref.zrange(0, -1, withscores=True)

        self.assertEqual(write_response.key, 1)
        self.assertEqual(write_response.data, None)

        self.assertEqual(read_response.key, i)
        self.assertEqual(read_response.data, None)

        pipe.execute()

        self.assertEqual(write_response.data, 1)
        self.assertEqual(read_response.data, [('a', now)])

    def test_model_multi(self):

        ids = []
        for i in xrange(1, 5):
            o = Foo(a='test')
            o.a = o.primary_key()
            self.assertEqual(o.exists(), False)
            o.save()
            self.assertEqual(o.exists(), True)
            ids.append(o.primary_key())

        # throw in some ids to fetch that don't exist
        missing = [str(uuid4()) for _ in xrange(1, 5)]

        objects = []
        pipe = hbom.Pipeline()
        for i in ids:
            o = Foo.ref(i, pipe=pipe)
            objects.append(o)
            self.assertEqual(o.exists(), False)

        empty_objects = []
        for i in missing:
            o = Foo.ref(i, pipe=pipe)
            empty_objects.append(o)
            self.assertEqual(o.exists(), False)

        pipe.execute()
        # pp([object.to_dict() for object in objects ])

        for o in objects:
            self.assertEqual(o.a, o.primary_key())
            self.assertEqual(o.exists(), True)

        for o in empty_objects:
            self.assertEqual(o.exists(), False)

    def test_model_multi_thread(self):

        foo_ids = []
        bazz_ids = []
        quux_ids = []
        for i in xrange(1, 5):
            o = Foo(a='test')
            o.a = o.primary_key()
            o.save()
            foo_ids.append(o.primary_key())
            o = Bazz()
            o.a = o.primary_key()
            o.save()
            bazz_ids.append(o.primary_key())
            o = Quux()
            o.a = o.primary_key()
            o.save()
            quux_ids.append(o.primary_key())

        objects = [Foo.ref(i) for i in foo_ids] + \
                  [Bazz.ref(i) for i in bazz_ids] + \
                  [Quux.ref(i) for i in quux_ids]

        hbom.Pipeline().hydrate(objects)
        for o in objects:
            self.assertEqual(o.a, o.primary_key())
            self.assertTrue(o.exists())

    def test_model_ref_pipeline(self):

        foo_ids = []
        bazz_ids = []
        quux_ids = []
        for i in xrange(1, 5):
            o = Foo(a='test')
            o.a = o.primary_key()
            o.save()
            foo_ids.append(o.primary_key())
            o = Bazz()
            o.a = o.primary_key()
            o.save()
            bazz_ids.append(o.primary_key())
            o = Quux()
            o.a = o.primary_key()
            o.save()
            quux_ids.append(o.primary_key())

        pipe = hbom.Pipeline()
        objects = [Foo.ref(i, pipe=pipe) for i in foo_ids] + \
                  [Bazz.ref(i, pipe=pipe) for i in bazz_ids] + \
                  [Quux.ref(i, pipe=pipe) for i in quux_ids]
        pipe.execute()

        for o in objects:
            self.assertEqual(o.a, o.primary_key())
            self.assertTrue(o.exists())

    def test_multi_thread_error(self):

        foo_ids = []
        err_ids = []
        for i in xrange(1, 5):
            o = Foo(a='test')
            o.a = o.primary_key()
            o.save()
            foo_ids.append(o.primary_key())
            o = ErrorModel()
            err_ids.append(o.primary_key())

        foo_objects = [Foo.ref(i) for i in foo_ids]
        error_objects = [ErrorModel.ref(i) for i in err_ids]
        o = foo_objects + error_objects

        exception = None
        try:
            hbom.Pipeline().hydrate(o)
        except Exception, e:
            exception = e

        self.assertNotEqual(exception, None)
        # pp([object.to_dict() for object in objects ])

        for o in foo_objects:
            self.assertEqual(o.a, o.primary_key())

        for o in error_objects:
            self.assertEqual(o.a, None)


if __name__ == '__main__':
    unittest.main()
