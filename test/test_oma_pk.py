#!/usr/bin/env python

from setup import hbom, Toma
import unittest
import uuid


class OmaPkModel(Toma):
    my_id = hbom.StringField(
        default=lambda: str(uuid.uuid4()), primary=True)


class TestPK(unittest.TestCase):
    def test_pk_instantiate(self):
        x = OmaPkModel()
        assert x.my_id


if __name__ == '__main__':
    unittest.main()
