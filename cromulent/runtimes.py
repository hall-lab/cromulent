import json, numpy, os, subprocess, sys
from datetime import datetime

def runtimes(metadata_fn, ops_dn):
    if not os.path.exists(metadata_fn):
        raise Exception("No ops ids file found at {}".format(ops_ids_fn))
    with open(metadata_fn, "r") as f:
        metadata = json.load(f)

    steps = []
    for name in metadata["calls"].keys():
        run_times = []
        total = len(metadata["calls"][name])
        for job in metadata["calls"][name]:
            jobid = job.get("jobId", None)
            if not jobid:
                continue
            ops_id = os.path.basename(jobid)
            ops_fn = os.path.join(ops_dn, ops_id)
            detail = get_ops_detail(ops_fn, jobid)
            startTime = detail["metadata"].get("startTime", None)
            endTime = detail["metadata"].get("endTime", None)
            if startTime and endTime:
                run_times.append(endTime - startTime)
        steps.append({
            "name": name,
            "mean": None,
            "total": len(run_times),
            })
        if len(run_times) > 0:
            steps[-1]["mean"] = numpy.mean(run_times)
    return steps
    
#-- runtimes

def get_ops_detail(ops_fn, jobid):
    ops_out = ""
    if not os.path.exists(ops_fn):
        cmd = ["gcloud", "alpha", "genomics", "operations", "describe", "--format=json(metadata.createTime,metadata.endTime,metadata.startTime,error.code,error.message,metadata.pipeline.resources.virtualMachine.disks,metadata.pipeline.resources.virtualMachine.machineType)", jobid]
        ops_out = subprocess.check_output(cmd)
    else:
        with open(ops_fn, "r") as f:
            ops_out = f.read()

    time_format = "%Y-%m-%dT%H:%M:%S"
    detail = json.loads(ops_out)
    for k in set(["createTime", "startTime", "endTime"]):
        v = detail["metadata"].get(k, None)
        if v is None:
            continue
        detail["metadata"][k] = datetime.strptime(v.split(".")[0].lstrip("'"), time_format)

    return detail

#-- get_ops_detail
