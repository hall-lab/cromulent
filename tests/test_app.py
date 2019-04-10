import unittest

import os, sys

from .context import cromulent
import cromulent.app as app

class CromulentAppTest(unittest.TestCase):

    def test_init(self):
        theapp = app.CromulentApp("tests/data/cromulent/app/jes.conf")
        self.assertIsNotNone(theapp)
        self.assertIsNotNone(theapp.config)
        self.assertIsNone(theapp.db)
        self.assertEqual(theapp.config['database']['db']['user'], 'cromwell')
        self.assertEqual(theapp.config['database']['db']['password'], 'words')

        theapp = app.CromulentApp()
        self.assertIsNone(theapp.config)
        self.assertIsNone(theapp.db)

    def test_init_fails(self):
        with self.assertRaises(IOError) as cm:
            app.CromulentApp("/jes.conf")
        self.assertTrue("No such file or directory" in cm.exception)

    def test_connect(self):
        theapp = app.CromulentApp("tests/data/cromulent/app/sqlite.conf")
        self.assertIsNotNone(theapp)
        self.assertIsNotNone(theapp.config)
        self.assertIsNone(theapp.db)
        self.assertEqual(theapp.config['database']['db']['file'], 'tests/data/cromulent/app/test.db')
        db = theapp.connect()
        self.assertIsNotNone(db)
        self.assertIsNotNone(theapp.db)

    def test_connect_fail(self):
        theapp = app.CromulentApp()
        with self.assertRaises(Exception) as cm:
            theapp.connect()
        self.assertTrue("No configuration found to connect to database!" in cm.exception)

# -- CromulentAppTest


if __name__ == '__main__':
    unittest.main(verbosity=2)
