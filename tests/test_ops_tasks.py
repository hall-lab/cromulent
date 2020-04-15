import os, unittest
from click.testing import CliRunner

from cromulent.ops_tasks import ops_tasks_cmd

class OpsTasksTest(unittest.TestCase):
    def setUp(self):
        self.data_dn = os.path.join(os.path.dirname(__file__), "data", "ops")
        self.metadata_fn = os.path.join(self.data_dn, "metadata.json")
        self.job_ids = [
             "projects/washu-genome-inh-dis-analysis/operations/16837616465221457742",
             "projects/washu-genome-inh-dis-analysis/operations/1823025369343969712",
             "projects/washu-genome-inh-dis-analysis/operations/12466924758626841844",
        ]

    def test1_ops_tasks_cmd(self):
        runner = CliRunner()
        result = runner.invoke(ops_tasks_cmd, ["--help"])
        self.assertEqual(result.exit_code, 0)

        result = runner.invoke(ops_tasks_cmd, [self.metadata_fn])
        try:
            self.assertEqual(result.exit_code, 0)
        except:
            print(result.output)
            raise
        expected_output = """TASK_NAME                             SHARDS    ATTEMPTS
----------------------------------  --------  ----------
JointGenotyping.CheckSamplesUnique         1           1
JointGenotyping.CollectGVCFs               3           4
"""
        self.assertEqual(result.output, expected_output)

# -- OpsTasksTest

if __name__ == '__main__':
    unittest.main(verbosity=2)

#-- __main__
