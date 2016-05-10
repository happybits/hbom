#!/usr/bin/env python

# std-lib
import time
import unittest
import os
from uuid import uuid4

import time

# test harness
from setup import generate_uuid

# test-harness
from setup_couchbase import (
    hbom,
    couchbase,
    CouchbaseLite,
    skip_if_couchbase_disabled,
    TEST_DIR,
)

from couchbase.bucket import Bucket as CouchbaseBucket

couchbase_lite = CouchbaseLite()

def tearDownModule():
    global couchbase_lite
    couchbase_lite = None

@skip_if_couchbase_disabled
class TestSave(unittest.TestCase):
    def test_save(self):
        bucket = couchbase_lite.bucket
        print bucket
        res = bucket.upsert('foo', {'bar': 1, 'bazz': 2})

        print bucket.get('foo')

        print bucket.upsert('foo', {'bar': 1, 'bazz': 2}, cas=res.cas)


if __name__ == '__main__':
    unittest.main(verbosity=2)
