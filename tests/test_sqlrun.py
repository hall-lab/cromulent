import unittest

import os, sys

from .context import cromulent
import cromulent.app as app
import cromulent.sqlrun as sqlrun

class CromulentSqlrunTest(unittest.TestCase):

    def test_runsql(self):
        theapp = app.CromulentApp("tests/data/cromulent/app/sqlite.conf")
        db = theapp.connect()
        sqlrun.run(db, "select.sql")

# -- CromulentSqlrunTest

if __name__ == '__main__':
    unittest.main(verbosity=2)
