from __future__ import division

from pprint import pprint
import json, logging, math, functools
from collections import defaultdict

from gcloud import GenomicsOperation, OperationCostCalculator

import requests


class Execution(object):

    def __init__(self, json):
        self.json = json

    def status(self):
        return self.json['executionStatus']

    def shard(self):
        return self.json['shardIndex']

    def jobid(self):
        return self.json.get('jobId', None)

    def __str__(self):
        return json.dumps(self.json, sort_keys=True, indent=4, separators=(',', ': '))


class Metadata(object):

    def __init__(self, metadata):
        self.json_doc = metadata

    def calls(self):
        return {
            k: [Execution(x) for x in v]
            for k, v in self.json_doc['calls'].iteritems()
            }

class Server(object):
    def __init__(self, host="localhost", port=8000):
        self.host = host
        self.port = port

    def _get_base_url(self):
        return 'http://{}:{}'.format(self.host, self.port)

    def is_accessible(self):
        base_url = self._get_base_url()
        url = '/'.join([base_url, 'engine', 'v1', 'version'])
        try:
            r = requests.get(url)
        except requests.exceptions.ConnectionError as e:
            return False

        if r.status_code != 200:
            return False

        return True

    def get_workflow_metadata(self, workflow_id):
        base_url = self._get_base_url()
        url = '/'.join([base_url,
                        'api',
                        'workflows',
                        'v1',
                        workflow_id,
                        'metadata?expandSubWorkflows=false'])

        logging.info("Fetching workflow metadata: {}".format(workflow_id))
        r = requests.get(url)
        if r.status_code != 200:
            logging.error('Error retrieving workflow metadata: {}'.format(r.json()['message']))
            raise Exception(r.json()['message'])
        logging.debug("Obtained workflow metadata")
        return r.json()

    def get_workflow_status(self, workflow_id):
        base_url = self._get_base_url()
        url = '/'.join([base_url,
                        'api',
                        'workflows',
                        'v1',
                        workflow_id,
                        'status'])

        logging.debug("Fetching workflow status: {}".format(workflow_id))
        r = requests.get(url)
        logging.debug("Obtained workflow status")
        return r.json()['status']

    def get_workflow_execution_status(self, workflow_id):
        base_url = self._get_base_url()
        url = '/'.join([base_url,
                        'api',
                        'workflows',
                        'v1',
                        workflow_id,
                        'metadata?includeKey=executionStatus'])

        logging.debug("Fetching workflow metadata: {}".format(workflow_id))
        r = requests.get(url)
        status = r.json()
        logging.debug("Obtained workflow metadata")
        summary = defaultdict(int)
        for call in status['calls']:
            for task in status['calls']:
                for execution in status['calls'][task]:
                    status = execution['executionStatus']
                    summary[status] += 1
        return summary


class CostEstimator(object):

    def __init__(self, cromwell_server, google):
        self.google = google
        self.cromwell_server = cromwell_server

    def get_operation_metadata(self, name):
        return self.google.get_genomics_operation_metadata(name)

    @staticmethod
    def dollars(raw_cost):
        return math.ceil(raw_cost * 100) / 100

    def is_execution_subworkflow(self, execution):
        if 'subWorkflowId' in execution or 'subWorkflowMetadata' in execution:
            return True
        return False

    def get_calls(self, metadata):
        return metadata['calls']

    def get_subworkflow_metadata(self, execution):
        try:
            return execution['subWorkflowMetadata']
        except KeyError:
            # retrieve subworkflow
            wfid = execution['subWorkflowId']
            meta = self.cromwell_server.get_workflow_metadata(wfid)
            return meta

    def get_subworkflow_id(self, execution):
        try:
            return execution['subWorkflowMetadata']['id']
        except KeyError:
            return execution['subWorkflowId']

    def get_cached_job(self, execution):
        cache = execution["callCaching"]["result"]
        logging.debug("        Cached -- see {}".format(cache))
        (old_wf_id, old_call_name, old_shard_index) = (cache.split(' '))[2].split(':')
        old_metadata = self.cromwell_server.get_workflow_metadata(old_wf_id)
        proper_shard_index = int(old_shard_index)
        job_id = old_metadata['calls'][old_call_name][proper_shard_index]['jobId']
        return job_id

    def calculate_cost(self, metadata):
        calls = self.get_calls(metadata)

        summary = {}
        subworkflow_summary_costs = {}

        for task in calls:
            logging.debug("Processing {}".format(task))
            executions = calls[task]
            task_costs = defaultdict(int)
            for e in executions:
                shard = e['shardIndex']
                logging.debug("    Shard: {}".format(shard))
                if self.is_execution_subworkflow(e):
                    subworkflow_id = self.get_subworkflow_id(e)
                    logging.debug("    Entering Subworkflow: {} / {}".format(shard, subworkflow_id))
                    subworkflow_summary_costs = self.calculate_cost(self.get_subworkflow_metadata(e))
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
                    #cost = self.dollars(self.calculator.cost(op))
                    cost = self.google.estimate_genomics_operation_cost(op)
                    logging.debug('            cost: {}'.format(cost))
                    task_costs[shard] += cost

            if task_costs:
                total_cost = sum(task_costs.values())
                logging.debug("    Total Task Cost: {}".format(total_cost))
                if task in summary:
                    summary[task]['total-cost'] += total_cost
                    summary[task]['items'].append(task_costs)
                else:
                    summary[task] = { 'total-cost': total_cost, 'items' : [task_costs] }

        return summary
