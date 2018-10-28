from __future__ import division, print_function
from pprint import pprint
from functools import partial
import json

from clint.textui import puts, indent, colored
from tabulate import tabulate

def standard_cost_report(wf_id, json_costs, display_nano_dollars):
    units = partial(dollar_units, display_nano_dollars)
    display = partial(display_dollars, display_nano_dollars)
    puts('=== Workflow: {} ==='.format(wf_id))
    puts()
    headers = ['task', '# called', 'cpu', 'mem', 'disk', 'total', 'avg. cost']
    (total_cost, total_cpu_cost, total_mem_cost, total_disk_cost) = \
        (0.0, 0.0, 0.0, 0.0)

    table_rows = []
    total_calls = 0
    for k in sorted(json_costs.keys()):
        task_cpu_cost = units(json_costs[k]['cpu'])
        task_mem_cost = units(json_costs[k]['mem'])
        task_disk_cost = units(json_costs[k]['disk'])
        task_cost = units(json_costs[k]['total-cost'])

        total_cost += task_cost
        total_cpu_cost += task_cpu_cost
        total_mem_cost += task_mem_cost
        total_disk_cost += task_disk_cost

        total_task_calls = 0
        for item in json_costs[k]['items']:
            total_task_calls += len(item.keys())
        total_calls += total_task_calls

        avg_task_cost = task_cost / float(total_task_calls)
        table_rows.append([
            k,
            total_task_calls,
            display(task_cpu_cost),
            display(task_mem_cost),
            display(task_disk_cost),
            display(task_cost),
            display(avg_task_cost)
        ])

    unit = 'nano dollars (USD)' if display_nano_dollars else 'dollars (USD)'
    with indent(4, quote=''):
        puts("Prices in: '{}'".format(unit))
        puts()
        if display_nano_dollars:
            puts(tabulate(table_rows, headers, tablefmt="simple", floatfmt="8.4e"))
        else:
            puts(tabulate(table_rows, headers, tablefmt="simple", floatfmt=".3f"))

    puts()
    with indent(4, quote=''):
        puts('= Summary ======================')
        puts(colored.blue("       cpu  : {}".format(display(total_cpu_cost))))
        puts(colored.blue("       mem  : {}".format(display(total_mem_cost))))
        puts(colored.blue("       disk : {}".format(display(total_disk_cost))))
        puts(colored.green("Total Cost  : {}".format(display(total_cost))))
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

# -- Helper functions ----------------------------------------------------------

# convert nano dollars to standard US dollars
def dollar_units(display_nano, amount):
    if display_nano:
        return amount
    return amount * 1e-9

def display_dollars(display_nano, amount):
    if display_nano:
        return "{:>8.4e}".format(amount)
    return "{:>8.3f}".format(amount)
