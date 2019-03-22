from __future__ import division

from pprint import pprint
import json, logging, math, functools

from gcloud import GenomicsOperation

import requests

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

    def get_workflow_metadata(self, workflow_id, lite=False):
        base_url = self._get_base_url()
        url = '/'.join([base_url,
                        'api',
                        'workflows',
                        'v1',
                        workflow_id,
                        'metadata'])
        url_params = {}
        if lite:
            logging.debug("Applying metadata lite mode request params")
            params = {
                'expandSubWorkflows' : 'false',
                'includeKey' : 'jobId',
                'includeKey' : 'callRoot',
                'includeKey' : 'executionStatus',
                'includeKey' : 'stderr',
                'includeKey' : 'failures',
                'includeKey' : 'inputs',
                'includeKey' : 'callCaching',
                'includeKey' : 'outputs',
                'includeKey' : 'status',
                'includeKey' : 'workflowName',
                'includeKey' : 'workflowRoot',
                'includeKey' : 'submission',
                'includeKey' : 'start'
            }
        else:
            params = {
                'expandSubWorkflows' : 'false',
            }

        logging.info("Fetching workflow metadata: {}".format(workflow_id))
        r = requests.get(url, params=params)
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

    def abort_workflow(self, workflow_id):
        base_url = self._get_base_url()
        url = '/'.join([base_url,
                        'api',
                        'workflows',
                        'v1',
                        workflow_id,
                        'abort'])

        logging.debug("Attempting to abort workflow: {}".format(workflow_id))
        r = requests.post(url)
        logging.debug("Received server reply")
        return r.json()

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
        summary = {}
        for call in status['calls']:
            for task in status['calls']:
                for execution in status['calls'][task]:
                    state = execution['executionStatus']
                    if state in summary:
                        summary[state] += 1
                    else:
                        summary[state] = 1
        return summary

    def get_workflow_input_outputs(self, workflow_id):
        base_url = self._get_base_url()
        url = '/'.join([base_url,
                        'api',
                        'workflows',
                        'v1',
                        workflow_id,
                        'metadata?includeKey=executionStatus&includeKey=inputs&includeKey=outputs'])

        logging.debug("Fetching workflow metadata: {}".format(workflow_id))
        r = requests.get(url)
        logging.debug("Obtained workflow metadata")
        return r.json()

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

    def calculate_cost(self, metadata, tier_scheme='all'):
        # tier_scheme can be on of the following:
        # 1.  all       -- assume starting workflow in a new project
        #                  and include all the relevant tiering pricing
        # 2.  no-free   -- use tiered-pricing, but remove any free-tiers
        # 3.  top-tier  -- only use the pricing on the last/top tier
        # 4.  max-price -- use only the tier with the highest price
        logging.info("Using price tiering scheme: '{}'".format(tier_scheme))
        calls = self.get_calls(metadata)

        summary = {}
        subworkflow_summary_costs = {}

        for task in calls:
            logging.debug("Processing {}".format(task))
            executions = calls[task]
            task_costs = None
            for e in executions:
                shard = e['shardIndex']
                logging.debug("    Shard: {}".format(shard))
                if self.is_execution_subworkflow(e):
                    subworkflow_id = self.get_subworkflow_id(e)
                    logging.debug("    Entering Subworkflow: {} / {}".format(shard, subworkflow_id))
                    subworkflow_summary_costs = self.calculate_cost(self.get_subworkflow_metadata(e), tier_scheme=tier_scheme)
                    for task in subworkflow_summary_costs:
                        if task in summary:
                            summary[task]['cpu'] += subworkflow_summary_costs[task]['cpu']
                            summary[task]['mem'] += subworkflow_summary_costs[task]['mem']
                            summary[task]['disk'] += subworkflow_summary_costs[task]['disk']
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
                    cost = self.google.estimate_genomics_operation_cost(op, tier_scheme)
                    logging.debug('            cost: {}'.format(cost))

                    if task_costs is None:
                        task_costs = {}

                    if shard not in task_costs:
                        task_costs[shard] = {'cpu': 0.0, 'mem': 0.0, 'disk': 0.0}

                    task_costs[shard]['cpu']  += cost['cpu']
                    task_costs[shard]['mem']  += cost['mem']
                    task_costs[shard]['disk'] += cost['disk']

            if task_costs:
                cpu_costs  = sum([c['cpu'] for c in task_costs.values()])
                mem_costs  = sum([c['mem'] for c in task_costs.values()])
                disk_costs = sum([c['disk'] for c in task_costs.values()])
                total_cost = cpu_costs + mem_costs + disk_costs
                logging.debug("    Total Task Cost: {}".format(total_cost))
                if task in summary:
                    summary[task]['cpu']  += cpu_costs
                    summary[task]['mem']  += mem_costs
                    summary[task]['disk'] += disk_costs
                    summary[task]['total-cost'] += total_cost
                    summary[task]['items'].append(task_costs)
                else:
                    summary[task] = {
                        'cpu' : cpu_costs,
                        'mem' : mem_costs,
                        'disk' : disk_costs,
                        'total-cost': total_cost,
                        'items' : [task_costs]
                    }

        return summary
