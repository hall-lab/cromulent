from __future__ import division
from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

from gcloud import GenomicsOperation, OperationCostCalculator, generate_gcp_compute_pricelist
import cromwell
from collections import defaultdict

import logging
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
                logging.debug('operation: {}'.format(op))
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
        logging.debug("        Cached -- see {}".format(cache))
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
            logging.debug("Processing {}".format(task))
            executions = calls[task]
            task_costs = {}
            for e in executions:
                shard = e['shardIndex']
                logging.debug("    Shard: {}".format(shard))
                if self.is_execution_subworkflow(e):
                    subworkflow_id = e['subWorkflowMetadata']['id']
                    logging.debug("    Entering Subworkflow: {} / {}".format(shard, subworkflow_id))
                    subworkflow_summary_costs = self.alt_calculate_cost(self.get_subworkflow_metadata(e))
                    for task in subworkflow_summary_costs:
                        if task in summary:
                            summary[task]['total-cost'] += subworkflow_summary_costs[task]['total-cost']
                            summary[task]['items'].extend(subworkflow_summary_costs[task]['items'])
                        else:
                            summary[task] = subworkflow_summary_costs[task]
                else:
                    job_id = e.get('jobId', None)
                    if job_id is None:
                        job_id = self.get_cached_job(e)
                    op = GenomicsOperation(self.get_operation_metadata(job_id))
                    logging.debug('            operation: {}'.format(op))
                    cost = self.dollars(self.calculator.cost(op))
                    logging.debug('            cost: {}'.format(cost))
                    task_costs[shard] = cost

            if task_costs:
                total_cost = sum(task_costs.values())
                logging.debug("    Total Task Cost: {}".format(total_cost))
                if task in summary:
                    summary[task]['total-cost'] += total_cost
                    summary[task]['items'].append(task_costs)
                else:
                    summary[task] = { 'total-cost': total_cost, 'items' : [task_costs] }

        return summary

def ideal_workflow_cost(metadata_path=None,
                        workflow_id=None,
                        pricelist=None,
                        host='localhost',
                        port=8000):
    # setup the server object
    # decorate the cromwell.Server class function
    cromwell.Server.get_workflow_metadata = \
        memoize(cromwell.Server.get_workflow_metadata)
    server = cromwell.Server(host, port)

    logging.info("Checking if we have access to the cromwell server")
    if not server.is_accessible():
        msg = "Could not access the cromwell server!  Please ensure it is up!"
        logging.error(msg)
        raise Exception(msg)

    # derive the pricelist
    if pricelist is None:
        logging.info("Obtaining the compute price list from Google Cloud")
        pricelist = generate_gcp_compute_pricelist()

    # derive the metadata
    metadata = None
    if metadata_path:
        msg = "Loading the workflow metadata from : {}".format(metadata_path)
        logging.info(msg)
        with open(metadata_path) as f:
            metadata = json.load(f)
    else:
        logging.info("Fetching metadata from cromwell")
        metadata = server.get_workflow_metadata(workflow_id)
        logging.info("Fetched metadata from cromwell")

    if metadata is None:
        msg = "Could not derive workflow metadata"
        logger.error(msg)
        raise Exception(msg)

    # perform the calculations
    calc = CromwellCostCalculator(server, pricelist)
    logging.info("Starting cost calculations")
    cost = calc.alt_calculate_cost(metadata)
    logging.info("Finished cost calculations")

    return cost
