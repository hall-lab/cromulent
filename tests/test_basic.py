from .context import cromulent

import unittest

class BasicTestSuite(unittest.TestCase):
    """Basic test"""

    def test_simple(self):
        assert True

if __name__ == '__main__':
    unittest.main()
