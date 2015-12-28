#!/usr/bin/env python

from setup import hbom, generate_uuid
import unittest


class OmaPkModel(hbom.BaseModel):
    my_id = hbom.StringField(default=generate_uuid, primary=True)

    def _apply_changes(self, old, new, pipe=None):
        response = self._calc_changes(old, new)
        self._change_state = response
        return response['changes']


class TestPK(unittest.TestCase):
    def test_pk_instantiate(self):
        x = OmaPkModel()
        assert x.my_id


if __name__ == '__main__':
    unittest.main()
