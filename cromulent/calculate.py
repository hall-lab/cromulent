from __future__ import division

import logging
import json
import functools

import cromwell
import gcloud

def memoize(func):
    cache = {}

    @functools.wraps(func)
    def memoized_func(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    return memoized_func


def ideal_workflow_cost_alt(metadata_path=None,
                        workflow_id=None,
                        sku_path=None,
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

    # setup the google services and skus information
    gcloud.GoogleServices.get_machine_types = \
        memoize(gcloud.GoogleServices.get_available_compute_types)
    google = gcloud.GoogleServices(sku_path)

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
    estimator = cromwell.CostEstimator(server, google)
    logging.info("Starting cost calculations")
    cost = estimator.calculate_cost(metadata)
    logging.info("Finished cost calculations")

    return cost
