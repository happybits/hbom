#!/usr/bin/env python

# std-lib
import time
import unittest
import re

# test harness
from setup import generate_uuid

# test-harness
from setup_redis import (
    hbom,
    clear_redis_testdata,
    default_redis_connection,
    skip_if_redis_disabled,
)


class Sample(hbom.RedisObject):

    class definition(hbom.Definition):
        id = hbom.StringField(primary=True, default=generate_uuid)
        a = hbom.IntegerField()
        b = hbom.IntegerField(default=7)
        req = hbom.StringField(required=True)
        created_at = hbom.FloatField(default=time.time)

    class storage(hbom.RedisHash):
        _keyspace = 'TT_s'
        _db = default_redis_connection


@skip_if_redis_disabled
class TestRedisDefinitionPersistence(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test(self):
        pipe = hbom.Pipeline()
        x = Sample.definition(id='x', req='test1')
        y = Sample.new(id='y', req='test2')
        Sample.save(x, pipe=pipe)
        Sample.save(y, pipe=pipe)
        pipe.execute()

        pipe = hbom.Pipeline()
        x = Sample.get('x', pipe=pipe)
        x_ref = Sample.ref('x')
        y = Sample.ref('y', pipe=pipe)
        z = Sample.get('z', pipe=pipe)

        self.assertFalse(x.exists())
        self.assertFalse(x_ref.exists())
        self.assertFalse(y.exists())
        self.assertFalse(z.exists())
        pipe.attach(x_ref)
        pipe.execute()

        self.assertTrue(x.exists())
        self.assertTrue(x_ref.exists())
        self.assertTrue(y.exists())
        self.assertFalse(z.exists())

        pipe = hbom.Pipeline()
        Sample.delete('x', pipe=pipe)
        Sample.delete('y', pipe=pipe)
        pipe.execute()

        pipe = hbom.Pipeline()
        x = Sample.get('x', pipe=pipe)
        y = Sample.get('y', pipe=pipe)
        z = Sample.get('z', pipe=pipe)

        self.assertFalse(x.exists())
        self.assertFalse(y.exists())
        self.assertFalse(z.exists())

        pipe.execute()


class ColdStorageMock(dict):

    def set(self, k, v):
        self[k] = v

    def set_multi(self, mapping):
        for k, v in mapping.items():
            self.set(k, v)

    def get_multi(self, keys):
        return {k: self.get(k) for k in keys}

    def delete(self, k):
        try:
            del self[k]
        except KeyError:
            pass

    def delete_multi(self, keys):
        for k in keys:
            self.delete(k)

    def ids(self):
        for k in self.keys():
            yield k


class Foo(hbom.RedisObject):

    class definition(hbom.Definition):
        id = hbom.StringField(primary=True, default=generate_uuid)
        a = hbom.IntegerField()
        b = hbom.IntegerField(default=7)
        created_at = hbom.FloatField(default=time.time)

    class storage(hbom.RedisHash):
        _keyspace = 'FOO'
        _db = default_redis_connection

    coldstorage = ColdStorageMock()

    is_hot_key = re.compile(r'^[0-9]+\.[A-Za-z0-9\-\._]+$').match


class TestRedisColdStorage(unittest.TestCase):
    def setUp(self):
        clear_redis_testdata()

    def tearDown(self):
        clear_redis_testdata()

    def test(self):
        pipe = hbom.Pipeline()
        x = Foo.definition(id='x', a=1)
        y = Foo.new(id='y', a=2)
        z = Foo.new(id='0.zz', a=3)
        Foo.save(x, pipe=pipe)
        Foo.save(y, pipe=pipe)
        Foo.save(z, pipe=pipe)
        pipe.execute()
        Foo.freeze('x', 'y', '0.zz')
        data = Foo.coldstorage.get('x')
        self.assertEqual(data, Foo.storage('x').dump())
        self.assertAlmostEqual(Foo.storage('x').ttl(), 300, places=-1)
        self.assertEqual(Foo.storage('0.zz').ttl(), -1)
        self.assertTrue(Foo.is_hot_key('0.zz'))

        z = Foo.get('0.zz')
        self.assertEqual(z.a, 3)

        Foo.thaw('x', 'y')
        self.assertTrue(Foo.storage('x').exists())
        self.assertTrue(Foo.storage('y').exists())
        self.assertEqual(Foo.get('x').a, 1)
        self.assertEqual(Foo.get('y').a, 2)
        self.assertIsNone(Foo.coldstorage.get('x'))
        self.assertIsNone(Foo.coldstorage.get('y'))

        Foo.freeze('x', 'y')

        Foo.storage('x').delete()
        x = Foo.get('x')
        self.assertEqual(x.id, 'x')
        self.assertEqual(x.a, 1)
        Foo.delete('x')
        x = Foo.get('x')
        self.assertFalse(x.exists())
        self.assertIsNone(Foo.coldstorage.get('x'))
        self.assertIsNotNone(Foo.coldstorage.get('y'))


if __name__ == '__main__':
    unittest.main()
