#!/usr/bin/env python

# std lib
from builtins import range
import time
import unittest
from uuid import uuid4
import os

# test-harness
from unit_test_setup import generate_uuid
from setup_redis import(
    hbom,
    clear_redis_testdata,
    TEST_DIR,
    default_redis_connection,
    redislite,
    skip_if_redis_disabled
)


class Foo(hbom.RedisObject):
    class definition(hbom.Definition):
        id = hbom.StringField(primary=True, default=generate_uuid)
        a = hbom.StringField(required=True)

    _keyspace = 'TT_foo'
    _db = 'test'


class Bar(hbom.RedisSortedSet):
    _keyspace = 'TT_bar'
    a = hbom.StringField()
    _db = 'test'


class Bazz(hbom.RedisObject):
    _keyspace = 'TT_bazz'
    _db = 'test'

    class definition(hbom.Definition):
        id = hbom.StringField(primary=True, default=generate_uuid)
        a = hbom.StringField()


class Quux(hbom.RedisObject):
    _keyspace = 'TT_quux'
    _db = 'test_alt'

    class definition(hbom.Definition):
        id = hbom.StringField(primary=True, default=generate_uuid)
        a = hbom.StringField()


class Sample(hbom.RedisObject):
    class definition(hbom.Definition):
        a = hbom.StringField(primary=True, required=True)
        b = hbom.IntegerField()

    _keyspace = 'TT_sample'
    _db = 'test'


@skip_if_redis_disabled
class TestPipeline(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test_pipeline_model(self):
        pipe = hbom.Pipeline()
        i = 'abc123'
        now = int(time.time())
        s = Sample.new(a=i)
        s.b = now
        Sample.save(s, pipe=pipe)
        r = Sample.get(i, pipe=pipe)
        self.assertFalse(Sample.get(i).exists())
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

        pipe.execute()

        self.assertEqual(write_response, 1)
        self.assertEqual(read_response, [('a', now)])

    def test_model_multi(self):

        ids = []
        for i in range(1, 5):
            o = Foo.new(a='test')
            o.a = o.primary_key()
            self.assertEqual(o.exists(), False)
            Foo.save(o)
            self.assertEqual(o.exists(), True)
            ids.append(o.primary_key())

        # throw in some ids to fetch that don't exist
        missing = [str(uuid4()) for _ in range(1, 5)]

        objects = []
        pipe = hbom.Pipeline()
        for i in ids:
            o = Foo.get(i, pipe=pipe)
            objects.append(o)
            self.assertEqual(o.exists(), False)

        empty_objects = []
        for i in missing:
            o = Foo.get(i, pipe=pipe)
            empty_objects.append(o)
            self.assertEqual(o.exists(), False)

        pipe.execute()
        for o in objects:
            self.assertEqual(o.a, o.primary_key())
            self.assertEqual(o.exists(), True)

        for o in empty_objects:
            self.assertEqual(o.exists(), False)

    def test_model_multi_thread(self):

        foo_ids = []
        bazz_ids = []
        quux_ids = []
        for i in range(1, 5):
            i = "%s" % i
            o = Foo.new(a='test')
            o.a = o.primary_key()
            Foo.save(o)
            foo_ids.append(o.primary_key())
            o = Bazz.new()
            o.a = o.primary_key()
            Bazz.save(o)
            bazz_ids.append(o.primary_key())
            o = Quux.new()
            o.a = o.primary_key()
            Quux.save(o)
            quux_ids.append(o.primary_key())

        objects = [Foo.ref(i) for i in foo_ids] + \
                  [Bazz.ref(i) for i in bazz_ids] + \
                  [Quux.ref(i) for i in quux_ids]

        hbom.hydrate(objects)
        for o in objects:
            self.assertEqual(o.a, o.primary_key())
            self.assertTrue(o.exists())

    def test_model_ref_pipeline(self):

        foo_ids = []
        bazz_ids = []
        quux_ids = []
        for i in range(1, 5):
            o = Foo.new(a='test')
            o.a = o.primary_key()
            Foo.save(o)
            foo_ids.append(o.primary_key())
            o = Bazz.new()
            o.a = o.primary_key()
            Bazz.save(o)
            bazz_ids.append(o.primary_key())
            o = Quux.new()
            o.a = o.primary_key()
            Quux.save(o)
            quux_ids.append(o.primary_key())

        pipe = hbom.Pipeline()
        objects = [Foo.ref(i, pipe=pipe) for i in foo_ids] + \
                  [Bazz.ref(i, pipe=pipe) for i in bazz_ids] + \
                  [Quux.ref(i, pipe=pipe) for i in quux_ids]
        pipe.execute()

        for o in objects:
            self.assertEqual(o.a, o.primary_key())
            self.assertTrue(o.exists())


if __name__ == '__main__':
    unittest.main()
