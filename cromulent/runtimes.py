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
        cnt = 0
        for job in metadata["calls"][name]:
            jobid = job.get("jobId", None)
            if not jobid:
                continue
            cnt += 1
            if cnt % 1000 == 1:
                sys.stderr.write("{} {} of {}\n".format(name, cnt, total))
            ops_id = os.path.basename(jobid)
            ops_fn = os.path.join(ops_dn, ops_id)
            detail = get_ops_detail(ops_fn, jobid)
            runTime = detail.get("runTime", None)
            if runTime:
                run_times.append(runTime)
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
        cmd = ["gcloud", "alpha", "genomics", "operations", "describe", jobid]
        ops_out = subprocess.check_output(cmd)
    else:
        with open(ops_fn, "r") as f:
            ops_out = f.read()

    time_format = "%Y-%m-%dT%H:%M:%S"
    detail = {
        "runTime": None,
    }
    attrs = set(["wdl-task-name", "createTime", "startTime", "endTime"])
    for line in ops_out.splitlines():
        if len(attrs) == 0:
            break
        for k in list(attrs):
            if k in line:
                v = line.split(": ")[1]
                if "Time" in k:
                    v = datetime.strptime(v.split(".")[0].lstrip("'"), time_format)
                detail[k] = v
                attrs.remove(k)

    startTime = detail.get("startTime", None)
    endTime = detail.get("endTime", None)
    if startTime and endTime:
        detail["runTime"] = endTime - startTime

    return detail

#-- get_ops_detail
