import json, logging

import requests

from collections import defaultdict

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
