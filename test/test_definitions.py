#!/usr/bin/env python
import unittest
from setup import hbom


class TestDefinition(unittest.TestCase):

    def test_one(self):
        User = hbom.Definition('User', 'id, name, emails')


        u = User(1, name='john', emails=['jl@example.com', 'jl@test.com'])

        print u[0]
        print u.__dict__
        print u.id
        print getattr(u, 'id')

        u = User(2)
        print u.id



if __name__ == '__main__':
    unittest.main()
