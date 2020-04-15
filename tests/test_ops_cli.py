import os, subprocess, unittest
from click.testing import CliRunner

from cromulent.ops_cli import ops_cli, ops_list_cmd

class OpsCliTest(unittest.TestCase):
    def setUp(self):
        self.data_dn = os.path.join(os.path.dirname(__file__), "data", "ops")
        self.metadata_fn = os.path.join(self.data_dn, "metadata.json")
        self.job_ids = [
             "projects/washu-genome-inh-dis-analysis/operations/16837616465221457742",
             "projects/washu-genome-inh-dis-analysis/operations/1823025369343969712",
             "projects/washu-genome-inh-dis-analysis/operations/12466924758626841844",
        ]

    def test1_ops_cli(self):
        runner = CliRunner()
        result = runner.invoke(ops_cli)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(ops_cli, ["--help"])
        self.assertEqual(result.exit_code, 0)

    def test2_ops_list_cmd(self):
        runner = CliRunner()
        result = runner.invoke(ops_list_cmd, ["--help"])
        self.assertEqual(result.exit_code, 0)

        result = runner.invoke(ops_list_cmd, [self.metadata_fn, "--names", "NaN"])
        try:
            self.assertEqual(result.exit_code, 1)
        except:
            print(result.output)
            raise
        self.assertEqual(result.output, "")

        result = runner.invoke(ops_list_cmd, [self.metadata_fn])
        try:
            self.assertEqual(result.exit_code, 0)
        except:
            print(result.output)
            raise
        expected_output = """projects/washu-genome-inh-dis-analysis/operations/16837616465221457742 Failed
projects/washu-genome-inh-dis-analysis/operations/1823025369343969712 RetryableFailure
projects/washu-genome-inh-dis-analysis/operations/12466924758626841844 Done
"""
        self.assertEqual(result.output, expected_output)

        result = runner.invoke(ops_list_cmd, [self.metadata_fn, "--names", "JointGenotyping.CheckSamplesUnique"])
        try:
            self.assertEqual(result.exit_code, 0)
        except:
            print(result.output)
            raise
        self.assertEqual(result.output, "")

# -- CromulentCliTest

if __name__ == '__main__':
    unittest.main(verbosity=2)

#-- __main__
