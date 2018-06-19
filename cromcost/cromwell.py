from __future__ import print_function
import json

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
                        'metadata?expandSubWorkflows=true'])

        print("Fetching metadata for the first time for workflow: {}".format(workflow_id))
        r = requests.get(url)
        return r.json()


