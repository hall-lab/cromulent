import datetime, io, os, subprocess, sys, tempfile, unittest
from mock import patch

from cromulent.runtimes import runtimes, get_ops_detail

class CromulentRuntimesTest(unittest.TestCase):

    def setUp(self):
        self.data_dn = os.path.join(os.path.dirname(__file__), "data", "runtimes")
        self.metadata_fn = os.path.join(self.data_dn, "metadata.json")
        #self.temp_d = tempfile.TemporaryDirectory()

    #def tearDown(self):
        #self.temp_d.cleanup()

    @patch("subprocess.check_output")
    def test1_ops_detail_from_file(self, check_output_patch):
        jobid = "4894199199968681626"
        ops_fn = os.path.join(self.data_dn, jobid)
        ops = get_ops_detail(ops_fn, jobid)
        expected_detail = {
            'createTime': datetime.datetime(2020, 3, 20, 20, 27, 22),
            'endTime': datetime.datetime(2020, 3, 21, 3, 42, 53),
            'startTime': datetime.datetime(2020, 3, 20, 21, 57, 43),
            'wdl-task-name': 'genotypegvcfs',
        }
        expected_detail["runTime"] = expected_detail["endTime"] - expected_detail["startTime"]
        self.assertDictEqual(ops, expected_detail)
        check_output_patch.assert_not_called()

    @patch("subprocess.check_output")
    def test1_ops_detail_from_cmd(self, check_output_patch):
        jobid = "4894199199968681626"
        ops_fn = os.path.join(self.data_dn, jobid)
        with open(ops_fn, "r") as f:
            check_output_patch.return_value = f.read()
        detail = get_ops_detail(jobid, jobid) # pass in non-exising ops file
        expected_detail = {
            'createTime': datetime.datetime(2020, 3, 20, 20, 27, 22),
            'endTime': datetime.datetime(2020, 3, 21, 3, 42, 53),
            'startTime': datetime.datetime(2020, 3, 20, 21, 57, 43),
            'wdl-task-name': 'genotypegvcfs',
        }
        expected_detail["runTime"] = expected_detail["endTime"] - expected_detail["startTime"]
        self.assertDictEqual(detail, expected_detail)
        check_output_patch.assert_called_once_with(["gcloud", "alpha", "genomics", "operations", "describe", jobid])

    @patch("subprocess.check_output")
    def test2_runtimes(self, check_output_patch):
        jobid = "4894199199968681626"
        ops_fn = os.path.join(self.data_dn, jobid)
        steps = runtimes(metadata_fn=self.metadata_fn, ops_dn=self.data_dn)
        expected_steps = [
            {
                "name": "JointGenotyping.CheckSamplesUnique",
                "mean": None,
                "total": 0,
            },
            {
                "name": "JointGenotyping.CollectGVCFs",
                "mean": datetime.timedelta(seconds=50878, microseconds=500000),
                "total": 2,
            },
        ]
        self.assertEqual(len(steps), 2)
        self.assertDictEqual(steps[0], expected_steps[0])
        self.assertDictEqual(steps[1], expected_steps[1])
        #check_output_patch.assert_called_once_with(["gcloud", "alpha", "genomics", "operations", "describe", jobid])

# -- CromulentRuntimesTest

if __name__ == '__main__':
    unittest.main(verbosity=2)

#-- __main__
