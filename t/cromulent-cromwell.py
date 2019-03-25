import unittest

import sys
sys.path.insert(0, '../cromulent/')

import cromwell

class CromwellServerTest(unittest.TestCase):
    server = None

    def test(self):
        server = cromwell.Server()
        self.assertEqual(server.host, 'localhost')
        self.assertEqual(server.port, 8000)
        self.__class__.server = server

    def test2(self):
        self.assertIsNotNone(self.__class__.server)

if __name__ == '__main__':
    unittest.main(verbosity=2)
