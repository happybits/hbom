#!/usr/bin/env python
import unittest
from uuid import uuid4
from setup import hbom, clear_redis_testdata


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


if __name__ == '__main__':
    unittest.main()
