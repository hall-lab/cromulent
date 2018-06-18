from __future__ import division, print_function
from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

from gcloud import GenomicsOperation, OperationCostCalculator, generate_gcp_compute_pricelist
import cromwell
from collections import defaultdict

import json
import sys
import math
import argparse
import datetime
import functools

def memoize(func):
    cache = {}

    @functools.wraps(func)
    def memoized_func(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    return memoized_func

class CromwellCostCalculator(object):

    def __init__(self, cromwell_server, pricelist):
        credentials = GoogleCredentials.get_application_default()
        self.service = discovery.build('genomics', 'v1', credentials=credentials)
        self.calculator = OperationCostCalculator(pricelist)
        self.cromwell_server = cromwell_server

    def get_operation_metadata(self, name):
        request = self.service.operations().get(name=name)
        response = request.execute()
        return response

    @staticmethod
    def dollars(raw_cost):
        return math.ceil(raw_cost * 100) / 100

    def calculate_cost(self, metadata_json):
        metadata = cromwell.Metadata(metadata_json)

        total_cost = 0
        max_samples = -1
        summary_json = { 'tasks': [], 'total_cost': None, 'cost_per_shard': None }

        for task, executions in metadata.calls().iteritems():
            task_totals = defaultdict(int)
            for e in executions:
                if e.jobid() is None: continue
                op = GenomicsOperation(self.get_operation_metadata(e.jobid()))
                print('operation: {}'.format(op))
                task_totals[e.shard()] = task_totals[e.shard()] + self.dollars(self.calculator.cost(op))
                total_cost += self.dollars(self.calculator.cost(op))
            summary_json['tasks'].append({
                    'name': task,
                    'shards': len(task_totals),
                    'cost_per_shard': self.dollars(sum(task_totals.values())/len(task_totals)) if len(task_totals) != 0 else 0,
                    'total_cost': self.dollars(sum(task_totals.values()))
                    })
            max_samples = max(max_samples, len(task_totals))
        summary_json['total_cost'] = total_cost
        summary_json['cost_per_shard'] = total_cost / max_samples
        return summary_json

    def is_execution_subworkflow(self, execution):
        if "subWorkflowMetadata" in execution:
            return True
        return False

    def get_calls(self, metadata):
        return metadata['calls']

    def get_subworkflow_metadata(self, execution):
        return execution['subWorkflowMetadata']

    def get_cached_job(self, execution):
        cache = execution["callCaching"]["result"]
        print("        Cached -- see {}".format(cache))
        (old_wf_id, old_call_name, old_shard_index) = (cache.split(' '))[2].split(':')
        old_metadata = self.cromwell_server.get_workflow_metadata(old_wf_id)
        proper_shard_index = int(old_shard_index)
        job_id = old_metadata['calls'][old_call_name][proper_shard_index]['jobId']
        return job_id

    def alt_calculate_cost(self, metadata):
        calls = self.get_calls(metadata)

        summary = {}
        subworkflow_summary_costs = {}

        for task in calls:
            print("Processing {}".format(task))
            executions = calls[task]
            task_costs = {} 
            for e in executions:
                shard = e['shardIndex']
                print("    Shard: {}".format(shard))
                if self.is_execution_subworkflow(e):
                    print("    Entering Subworkflow: {}".format(shard))
#                    import pdb; pdb.set_trace()
                    subworkflow_summary_costs = self.alt_calculate_cost(self.get_subworkflow_metadata(e))
#                    import pdb; pdb.set_trace()
                    for task in subworkflow_summary_costs:
                        if task in summary:
                            summary[task]['total-cost'] += subworkflow_summary_costs[task]['total-cost']
                            summary[task]['items'].extend(subworkflow_summary_costs[task]['items'])
                        else:
                            summary[task] = subworkflow_summary_costs[task]
#                    import pdb; pdb.set_trace()
                else:
                    job_id = e.get('jobId', None)
                    if job_id is None:
                        job_id = self.get_cached_job(e)
                    op = GenomicsOperation(self.get_operation_metadata(job_id))
                    print('            operation: {}'.format(op))
                    cost = self.dollars(self.calculator.cost(op))
                    print('            cost: {}'.format(cost))
                    task_costs[shard] = cost

            if task_costs:
                total_cost = sum(task_costs.values())
                print("    Total Task Cost: {}".format(total_cost))
                if task in summary:
                    summary[task]['total-cost'] += total_cost
                    summary[task]['items'].append(task_costs)
                else:
                    summary[task] = { 'total-cost': total_cost, 'items' : [task_costs] }
#            else:
#                summary[task] = { 'total-cost': 0.0, 'items' : [{}] }

#            if subworkflow_summary_costs:
#                for task in subworkflow_summary_costs:
#                    if task in summary:
#                        summary[task]['total-cost'] += subworkflow_summary_costs[task]['total-cost']
#                        summary[task]['items'].append(subworkflow_summary_costs[task]['items'])
#                    else:
#                        summary[task] = subworkflow_summary_costs[task]

        return summary

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'metadata',
        type=argparse.FileType('r'),
        help='metadata from a cromwell workflow from which to estimate cost'
    )
    parser.add_argument(
        '--workflow-id',
        dest='workflow_id',
        help='the primary cromwell workflow-id to calculate costs on'
    )
    parser.add_argument(
        '--dump-pricelist',
        dest='dump_pricelist',
        type=argparse.FileType('w'),
        help='the primary cromwell workflow-id to calculate costs on'
    )
    args = parser.parse_args()

    #import pdb; pdb.set_trace()

    # decorate the cromwell.Server class function
    cromwell.Server.get_workflow_metadata = memoize(cromwell.Server.get_workflow_metadata)
    server = cromwell.Server()

    if not server.is_accessible():
        msg = "Could not access the cromwell server!  Please ensure it is up!"
        raise Exception(msg)

    metadata = None
    if args.metadata:
        metadata = json.load(args.metadata)
    else:
        metadata = server.get_workflow_metadata(args.workflow_id)

    pricelist = generate_gcp_compute_pricelist()
    if args.dump_pricelist:
        print(json.dumps(pricelist, indent=4, sort_keys=True), file=dump_pricelist)


    calc = CromwellCostCalculator(server, pricelist)
    cost = calc.alt_calculate_cost(metadata)
    print(json.dumps(cost, sort_keys=True, indent=4))
    total_cost = 0.0
    for k in cost.keys():
        task_cost = cost[k]['total-cost']
        total_cost += task_cost
        print("{} : {}".format(k, task_cost))
    print("Total Cost: {}".format(total_cost))
    # print('Total: ${0}'.format(cost['total_cost']))
    # print('Per Shard: ${0}'.format(cost['cost_per_shard']))
