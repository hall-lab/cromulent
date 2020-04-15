import click, json, os, tabulate, sys
from collections import OrderedDict

@click.command(short_help="show tasks from a workflow")
@click.argument("metadata_fn", type=click.STRING)
def ops_tasks_cmd(metadata_fn):
    """
    Show Workflow Tasks Info
    """
    if not os.path.exists(metadata_fn):
        raise Exception("Metadata file {} does not exist!".format(metadata_fn))
    with open(metadata_fn, "r") as f:
        metadata = json.load(f, object_pairs_hook=OrderedDict)

    rows = []
    for task_name in metadata["calls"].keys():
        attempts = len(metadata["calls"][task_name])
        shards = set()
        for task in metadata["calls"][task_name]:
            shards.add(task.get("shardIndex", -1))
        rows.append( map(str, [task_name, len(shards), attempts]) )
    sys.stdout.write( tabulate.tabulate(rows, ["TASK_NAME", "SHARDS", "ATTEMPTS"]) + "\n")
