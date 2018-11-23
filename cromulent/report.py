from __future__ import division, print_function
from pprint import pprint
from functools import partial
import json

import cromulent.utils as utils

from clint.textui import puts, indent, colored
from tabulate import tabulate
from cytoolz.itertoolz import frequencies, take
from cytoolz.curried import pipe, map, filter, get
from cytoolz.dicttoolz import merge, valmap, get_in

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
    print("    {} : {}".format(colored.green('Done'), summary.get('Done', 0)))
    print("    {} : {}".format(colored.red('Failed'), summary.get('Failed', 0)))
    for status in sorted(summary):
        if status not in ('Done', 'Failed'):
            print("    {} : {}".format(colored.yellow(status), summary[status]))

def workflow_report_types():
    dispatch = workflow_report_dispatcher()
    return dispatch.keys()

def workflow_report_dispatcher():
    dispatch = {
        'summary' : wf_summary,
        'failures': wf_failures,
    }
    return dispatch

def workflow_report(report, metadata, opts):
    dispatch = workflow_report_dispatcher()

    if report not in dispatch:
        msg = "Workflow report '{}' is not implemented!".format(report)
        logger.error(msg)
        raise Exception(msg)

    fn = dispatch[report]
    fn(metadata, opts)

def wf_summary(metadata, opts):
    overall_wf_attributes = (
        'id', 'status',
        'workflowName', 'workflowRoot',
        'submission', 'start'
    )

    (wf_id, wf_status, wf_name, wf_root, wf_submission, wf_start) = \
            [metadata[x] for x in overall_wf_attributes]

    wf_end = get('end', metadata, default="-")

    puts('')
    puts("ID         : {}".format(wf_id))
    puts("Status     : {}".format(wf_status))
    puts("Submit Time: {} (UTC)".format(wf_submission))
    puts("Start  Time: {} (UTC)".format(wf_start))
    puts("End    Time: {} (UTC)".format(wf_end))
    puts('')

    (calls, states, stats) = _get_wf_call_statuses(metadata)

    table = []
    for c in calls:
        counts = [ stats[c][s] for s in states ]
        row = [c]
        row.extend(counts)
        table.append(row)

    headers = ['call']
    headers.extend([ s for s in states ])
    print(tabulate(table, headers=headers))

def _get_wf_call_statuses(metadata):
    calls = metadata['calls'].keys()
    states = set([])
    call_stats = {}

    for c in calls:
        tasks = metadata['calls'][c]
        counts = pipe(tasks, map(get('executionStatus')),
                             frequencies)
        new_states = list(filter(lambda x: x not in states, counts.keys()))
        if new_states:
            for s in new_states: states.add(s)
        call_stats[c] = counts

    base_states = { s : 0 for s in states }

    final_stats = valmap(lambda d: merge(base_states, d), call_stats)
    return (calls, sorted(states), final_stats)

def wf_failures(metadata, opts):
    extra_opts = utils.parse_wf_report_opts(opts)

    fails = _get_wf_call_failures(metadata, extra_opts)

    if 'detail' in extra_opts:
        _generate_detail_wf_failure_report(fails, extra_opts)
    else:
        _generate_basic_wf_failure_report(fails, extra_opts)

def _generate_detail_wf_failure_report(fails, opts):
    for call in fails:
        for f in fails[call]:
            header = 'call: {} | shard: {} | jobId: {} | rc: {}'
            header = header.format(call, f['shard'], f['jobId'], f['rc'])

            puts()
            puts(colored.red(header))
            with indent(4, quote=''):
                puts(colored.yellow("--- Error Message: ---"))
                puts()
                with indent(2, quote='| '):
                    err_msg = f['err_msg']
                    puts(err_msg)
                puts()
                puts(colored.blue("--- Inputs: ---"))
                puts()
                with indent(2, quote=''):
                    inputs = json.dumps(f['inputs'], indent=4, sort_keys=True)
                    puts(inputs)
                puts()
                puts(colored.green("--- stderr: ---"))
                puts()
                with indent(2, quote=''):
                    puts(f['stderr'])
                puts()
                puts(colored.green("--- jes: ---"))
                puts()
                with indent(2, quote=''):
                    inputs = json.dumps(f['jes'], indent=4, sort_keys=True)
                    puts(inputs)
                puts()
                puts(colored.green("--- runtime: ---"))
                puts()
                with indent(2, quote=''):
                    inputs = json.dumps(f['runtime'], indent=4, sort_keys=True)
                    puts(inputs)
            puts()

def _generate_basic_wf_failure_report(fails, opts):
    headers = ['call', 'shard', 'jobId', 'rc', 'stderr']
    table = []
    for call in fails:
        for f in fails[call]:
            row = [ call, f['shard'], f['jobId'], f['rc'], f['stderr'] ]
            table.append(row)

    puts('')
    print(tabulate(table, headers=headers))

def _get_wf_call_failures(metadata, opts):
    calls = []
    if 'calls' in opts:
        calls = opts['calls'].split(',')
    else:
        calls = metadata['calls'].keys()

    jobids = None
    if 'jobids' in opts:
        jobids = set(opts['jobids'].split(','))

    fails = {}

    for c in calls:
        tasks = metadata['calls'][c]
        failures = pipe(tasks, filter(lambda x: get('executionStatus', x) == 'Failed'),
                               filter(lambda x: _valid_job_id(jobids, get('jobId', x))),
                               map(lambda x: { 'jobId'   : get('jobId', x),
                                               'inputs'  : get('inputs', x),
                                               'stderr'  : get('stderr', x),
                                               'shard'   : get('shardIndex', x),
                                               'err_msg' : get_in(['failures', 0, 'message'], x),
                                               'jes'     : get('jes', x),
                                               'runtime' : get('runtimeAttributes', x),
                                               'rc'      : get('returnCode', x) }),
                               list)
        fails[c] = failures

    return fails

def _valid_job_id(valid_jobid_text_set, full_jobid_name):
    if valid_jobid_text_set is None:
        return True

    for jobid_text in valid_jobid_text_set:
        if jobid_text in full_jobid_name:
            return True

    return False

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
