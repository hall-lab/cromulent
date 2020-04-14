import subprocess, unittest
from click.testing import CliRunner

from cromulent.cli import cli

class CromulentCliTest(unittest.TestCase):

    def test1_cromulent(self):
        runner = CliRunner()
        result = runner.invoke(cli)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(cli, ["--help"])
        self.assertEqual(result.exit_code, 0)

# -- CromulentCliTest

if __name__ == '__main__':
    unittest.main(verbosity=2)

#-- __main__
