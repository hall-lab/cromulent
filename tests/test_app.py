import unittest

import os, sys

from .context import cromulent
import cromulent.app as app

class CromulentAppTest(unittest.TestCase):

    def test(self):
        os.environ['CROMULENT_CONFIG'] = "tests/data/cromulent/app/jes.conf" # FIXME add to context
        theapp = app.CromulentApp()
        self.assertIsNotNone(theapp)


if __name__ == '__main__':
    unittest.main(verbosity=2)
