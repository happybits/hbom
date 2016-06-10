#!/usr/bin/env python

# std-lib
import time
import unittest

# test harness
from setup import generate_uuid

# test-harness
from setup_redis import (
    hbom,
    clear_redis_testdata,
    default_redis_connection,
    skip_if_redis_disabled,
)


class SampleDefinition(hbom.Definition):
    id = hbom.StringField(primary=True, default=generate_uuid)
    a = hbom.IntegerField()
    b = hbom.IntegerField(default=7)
    req = hbom.StringField(required=True)
    created_at = hbom.FloatField(default=time.time)


class Sample(hbom.RedisDefinitionPersistence):
    _definition = SampleDefinition
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
        x = SampleDefinition(id='x', req='test1')
        y = Sample.new(id='y', req='test2')
        Sample.save(x, pipe=pipe)
        Sample.save(y, pipe=pipe)
        pipe.execute()

        pipe = hbom.Pipeline()
        x = Sample.get('x', pipe=pipe)
        y = Sample.get('y', pipe=pipe)
        z = Sample.get('z', pipe=pipe)

        self.assertFalse(x.exists())
        self.assertFalse(y.exists())
        self.assertFalse(z.exists())

        pipe.execute()

        self.assertTrue(x.exists())
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

if __name__ == '__main__':
    unittest.main()
