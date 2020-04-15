import os, subprocess, tempfile, unittest
from click.testing import CliRunner
from mock import MagicMock, Mock, patch

from cromulent.ops_get import ops_get_cmd

class OpsGetTest(unittest.TestCase):
    def setUp(self):
        self.data_dn = os.path.join(os.path.dirname(__file__), "data", "ops")
        self.ops_ids = [
             "projects/washu-genome-inh-dis-analysis/operations/16837616465221457742",
             "projects/washu-genome-inh-dis-analysis/operations/1823025369343969712",
             "projects/washu-genome-inh-dis-analysis/operations/12466924758626841844",
        ]
        self.temp_d = tempfile.TemporaryDirectory()
        self.ops_fn = os.path.join(self.temp_d.name, "ops.out")
        with open(self.ops_fn, "w") as f:
            f.write("\n".join(self.ops_ids))

    @patch("subprocess.Popen")
    def test1_ops_get_cmd(self, popen_patch):
        runner = CliRunner()
        result = runner.invoke(ops_get_cmd, ["--help"])
        self.assertEqual(result.exit_code, 0)

        process = Mock()
        process.poll = MagicMock(return_value=0)
        popen_patch.return_value = process
        result = runner.invoke(ops_get_cmd, [self.ops_fn, self.temp_d.name])
        try:
            self.assertEqual(result.exit_code, 0)
        except:
            print(result.output)
            raise
        expected_output = """Get OPS from google cloud ... 
Total  3
Exist  0
Needed 3
Running gloud for needed ops ...
Base gcloud command: gcloud alpha genomics operations describe --format=json(metadata.createTime,metadata.endTime,metadata.startTime,error.code,error.message,metadata.pipeline.resources.virtualMachine.disks,metadata.pipeline.resources.virtualMachine.machineType)
Get OPS from google cloud ... DONE
"""
        self.assertEqual(result.output, expected_output)
        popen_patch.assert_called()


# -- OpsGetTest

if __name__ == '__main__':
    unittest.main(verbosity=2)

#-- __main__
