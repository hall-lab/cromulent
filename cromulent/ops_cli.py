import click, json, os, tabulate, sys

#import cromulanet.ops as ops

@click.group()
def ops_cli():
    """
    Commands to work with genomics operations 
    """
    pass

@click.command(short_help="grab genomics job ids from a workflow")
@click.argument("metadata_fn", type=click.STRING)
@click.option("--names", type=click.STRING, default="", help="Only get ops from these tasks. Give as comma separated list.")
def ops_list_cmd(metadata_fn, names):
    """
    List Genomics Operation Details in a Workflow

    Optionally, provide a white list of task names as a comma separated value.
    """
    if not os.path.exists(metadata_fn):
        raise Exception("Metadata file {} does not exist!".format(metadata_fn))
    with open(metadata_fn, "r") as f:
        metadata = json.load(f)

    task_names = set(metadata["calls"].keys())
    if names:
        names = set(names.split(","))
        if not names.issubset(task_names):
            raise Exception("Given task names {} were all not found in the given workflow tasks: {}".format(names, task_names))
        task_name = names

    for task_name in task_names:
        if names and task_name not in names:
            continue
        for task in metadata["calls"][task_name]:
            job_id = task.get("jobId", None)
            if job_id:
                sys.stdout.write("{}\n".format(job_id))
ops_cli.add_command(ops_list_cmd, name="list")
