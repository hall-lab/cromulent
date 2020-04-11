import datetime, io, os, subprocess, sys, tempfile, unittest
from mock import patch

from cromulent.runtimes import runtimes, get_ops_detail

class CromulentRuntimesTest(unittest.TestCase):

    def setUp(self):
        self.data_dn = os.path.join(os.path.dirname(__file__), "data", "runtimes")
        self.metadata_fn = os.path.join(self.data_dn, "metadata.json")
        self.jobids = {
            "Done": "12466924758626841844",
            "Failed": "16837616465221457742",
            "RetryableFailure": "1823025369343969712",
        }
        self.expected_times = {
            "createTime": datetime.datetime(2020, 3, 17, 15, 22, 33),
            "endTime": datetime.datetime(2020, 3, 19, 8, 16, 21),
            "startTime": datetime.datetime(2020, 3, 18, 12, 42, 39),
        }
        #self.temp_d = tempfile.TemporaryDirectory()

    #def tearDown(self):
        #self.temp_d.cleanup()

    @patch("subprocess.check_output")
    def test1_ops_detail_from_file(self, check_output_patch):
        jobid = self.jobids["Done"]
        ops_fn = os.path.join(self.data_dn, jobid)
        detail = get_ops_detail(ops_fn, jobid)
        got_times = {}
        for k in ("createTime", "startTime", "endTime"):
            got_times[k] = detail["metadata"][k]
        self.assertDictEqual(got_times, self.expected_times)
        check_output_patch.assert_not_called()

    @patch("subprocess.check_output")
    def test1_ops_detail_from_cmd(self, check_output_patch):
        jobid = self.jobids["Done"]
        ops_fn = os.path.join(self.data_dn, jobid)
        with open(ops_fn, "r") as f:
            check_output_patch.return_value = f.read()
        detail = get_ops_detail(jobid, jobid) # pass in non-exising ops file
        got_times = {}
        for k in ("createTime", "startTime", "endTime"):
            got_times[k] = detail["metadata"][k]
        self.assertDictEqual(got_times, self.expected_times)
        check_output_patch.assert_called_once_with(["gcloud", "alpha", "genomics", "operations", "describe", "--format=json(metadata.createTime,metadata.endTime,metadata.startTime,error.code,error.message,metadata.pipeline.resources.virtualMachine.disks,metadata.pipeline.resources.virtualMachine.machineType)", jobid])

    @patch("subprocess.check_output")
    def test2_runtimes(self, check_output_patch):
        sys.stderr = sys.__stderr__
        steps = runtimes(metadata_fn=self.metadata_fn, ops_dn=self.data_dn)
        sys.stderr = sys.__stderr__
        expected_steps = [
            {
                "name": "JointGenotyping.CheckSamplesUnique",
                "mean": None,
                "total": 0,
            },
            {
                "name": "JointGenotyping.CollectGVCFs",
                "mean": datetime.timedelta(seconds=39936, microseconds=500000),
                "total": 2,
            },
        ]
        self.assertEqual(len(steps), 2)
        self.assertDictEqual(steps[0], expected_steps[0])
        self.assertDictEqual(steps[1], expected_steps[1])

# -- CromulentRuntimesTest

if __name__ == '__main__':
    unittest.main(verbosity=2)

#-- __main__
