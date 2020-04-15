import click, collections, os, subprocess, sys

@click.command(short_help="fetch genomics operations details")
@click.argument("ops_fn", type=click.STRING)
@click.argument("ops_dn", type=click.STRING)
def ops_get_cmd(ops_fn, ops_dn):
    """
    Fetch Genomics Operation Details from Google Cloud
    """
    sys.stderr.write("Get OPS from google cloud ... \n")
    if not os.path.exists(ops_fn):
        raise Exception("Ops ids file {} does not exist!".format(ops_fn)) 

    total = 0
    ops_ids = collections.deque()
    with open(ops_fn, "r") as f:
        total += 1
        for ops_id in f:
            ops_id = ops_id.rstrip()
            ops_fn = os.path.join(ops_dn, os.path.basename(ops_id))
            if not os.path.exists(ops_fn):
                ops_ids.append(ops_id)
    sys.stderr.write("Total  {}\nExist  {}\nNeeded {}\n".format(total, (total - len(ops_ids)), len(ops_ids)))

    base_cmd = ["gcloud", "alpha", "genomics", "operations", "describe", "--format=json(metadata.createTime,metadata.endTime,metadata.startTime,error.code,error.message,metadata.pipeline.resources.virtualMachine.disks,metadata.pipeline.resources.virtualMachine.machineType)"]
    sys.stderr.write("Running gloud for needed ops ...\nBase gcloud command: {}\n".format(" ".join(base_cmd)))

    procs = collections.deque()
    while ops_ids or procs:
        while len(procs) < 5 and ops_ids: # run 5 at a time
            ops_id = ops_ids.popleft()
            ops_fn = os.path.join(ops_dn, os.path.basename(ops_id))
            with open(ops_fn, "w") as f:
                cmd = base_cmd + [ops_id]
                procs.append( subprocess.Popen(cmd, stdout=f) )

        procs_completed = []
        for proc in procs:
            if proc.poll() is not None:
                procs_completed.append(proc)

        for proc in procs_completed:
            procs.remove(proc)

    sys.stderr.write("Get OPS from google cloud ... DONE\n")

#-- ops_get_cmd
