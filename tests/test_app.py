import unittest

import os, sys

from .context import cromulent
import cromulent.app as app

class CromulentAppTest(unittest.TestCase):

    def test(self):
        theapp = app.CromulentApp("tests/data/cromulent/app/jes.conf")
        self.assertIsNotNone(theapp)


if __name__ == '__main__':
    unittest.main(verbosity=2)
