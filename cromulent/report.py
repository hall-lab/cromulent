from __future__ import division, print_function
from pprint import pprint
import json

from clint.textui import puts, indent, colored
from tabulate import tabulate

def standard_cost_report(wf_id, json_costs):
    puts('=== Workflow: {} ==='.format(wf_id))
    puts()
    headers = ['task', '# called', 'total cost', 'avg. cost']
    total_cost = 0.0

    table_rows = []
    total_calls = 0
    for k in sorted(json_costs.keys()):
        task_cost = json_costs[k]['total-cost']
        total_cost += task_cost
        total_task_calls = 0
        for item in json_costs[k]['items']:
            total_task_calls += len(item.keys())
        total_calls += total_task_calls
        avg_task_cost = task_cost / float(total_task_calls)
        table_rows.append([k, total_task_calls, task_cost, avg_task_cost])

    with indent(4, quote=''):
        puts(tabulate(table_rows, headers, tablefmt="simple"))

    puts()
    with indent(4, quote=''):
        puts('= Summary ======================')
        puts(colored.green("Total Cost  : {}".format(total_cost)))
        puts(colored.yellow("Total Calls : {}".format(total_calls)))
        puts('================================')

def raw_cost_report(wf_id, json_costs):
    data = { 'id' : wf_id, 'tasks' : json_costs }
    print(json.dumps(data, sort_keys=True, indent=4))

def display_workflow_status(wf_id, status):
    if status == 'Failed':
        color = colored.red
    elif status == 'Succeeded':
        color = colored.green
    else:
        color = colored.yellow
    print("{} : {}".format(wf_id, color(status)))

def display_workflow_execution_status(wf_id, summary):
    print("{} :".format(wf_id))
    print("    {} : {}".format(colored.green('Done'), summary['Done']))
    print("    {} : {}".format(colored.red('Failed'), summary['Failed']))
    for status in sorted(summary):
        if status not in ('Done', 'Failed'):
            print("    {} : {}".format(colored.yellow(status), summary[status]))
