from __future__ import print_function

import signal, sys, os, json, logging

import click

from cromulent.version import __version__

import cromulent.cromwell as cromwell
import cromulent.gcloud as gcloud
import cromulent.utils as utils
import cromulent.report as creport

logging.basicConfig(
    format='[%(asctime)s] : %(name)s : %(levelname)s : %(message)s',
    level=logging.INFO
)

# suppress the loggers from the other third-party modules
for name in logging.Logger.manager.loggerDict.keys():
    logging.getLogger(name).setLevel(logging.CRITICAL)

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__)
def cli():
    '''A collection of cromwell helpers.'''
    # to make this script/module behave nicely with unix pipes
    # http://newbebweb.blogspot.com/2012/02/python-head-ioerror-errno-32-broken.html
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

# -- Subcommands ---------------------------------------------------------------
@cli.command(name='sku-list',
             short_help="retrieve sku pricing info from the Google Cloud API")
@click.option('--output', type=click.Path(), default=None,
              help='Path to dump the raw JSON pricing information to')
def sku_list(output, raw):
    google = gcloud.GoogleServices()
    data = google.compute_engine_skus()
    skulist = json.dumps(data, indent=4, sort_keys=True)
    if output:
        with open(output, 'w') as f:
            print(skulist, file=f)
    else:
        print(skulist)

@cli.command(short_help='retrieve metadata for workflow-id')
@click.option('--output', type=click.Path(), default=None,
              help='Path to dump the raw JSON metadata information to')
@click.option('--host', type=click.STRING, default='localhost',
              help='cromwell web server host')
@click.option('--port', type=click.INT, default=8000,
              help='cromwell web server port')
@click.argument('workflow-id')
def metadata(workflow_id, output, host, port):
    cromwell.Server.get_workflow_metadata = \
        utils.memoize(cromwell.Server.get_workflow_metadata)
    server = cromwell.Server(host, port)
    if not server.is_accessible():
        msg = "Could not access the cromwell server!  Please ensure it is up!"
        raise Exception(msg)
    metadata = server.get_workflow_metadata(workflow_id)
    pretty_metadata = json.dumps(metadata, indent=4, sort_keys=True)
    if output:
        with open(output, 'w') as f:
            print(pretty_metadata, file=f)
    else:
        print(pretty_metadata)

@cli.command(short_help="estimate ideal workflow cost")
@click.option('--metadata', type=click.Path(exists=True), default=None,
              help=('Path to an existing (not-raw) '
                    'cromwell workflow metadata json file.'))
@click.option('--sku-list', type=click.Path(exists=True), default=None,
              help='Path to an existing sku pricing info json file.')
@click.option('--import-raw-cost-data', type=click.Path(exists=True),
              default=None,
              help='Import prior calculated raw cost data (in JSON format)')
@click.option('--workflow-id', type=click.STRING, default=None,
              help=('A cromwell workflow-id to fetch metadata from '
                    'the cromwell server'))
@click.option('--host', type=click.STRING, default='localhost',
              help='cromwell web server host')
@click.option('--port', type=click.INT, default=8000,
              help='cromwell web server port')
@click.option('--tier-scheme',
              type=click.Choice(['all', 'no-free', 'top-tier', 'max-price']),
              default='all',
              help='tiered pricing handling scheme')
@click.option('--report', type=click.Choice(['standard', 'raw']),
              default='standard',
              help='output report choice')
@click.option('--nanos', type=click.BOOL, is_flag=True, default=False,
              help='display costs in nano dollars')
@click.option('-v', '--verbose', count=True,
              help='verbosity level')
def estimate(metadata,
             sku_list,
             import_raw_cost_data,
             workflow_id,
             host,
             port,
             tier_scheme,
             report,
             nanos,
             verbose):
    if verbose:
        _setup_logging_level(verbose)

    # go straight to the report generation
    if import_raw_cost_data:
        with open(import_raw_cost_data, 'r') as f:
            costs = json.load(f)
        report_fn(costs['id'], costs['tasks'])
        sys.exit(0)

    # otherwise prepare to cost calcuate and then report
    if (metadata is None) and (workflow_id is None):
        sys.exit(("[err] Please specify either a "
                  "'--metadata' or '--workflow-id' option!"))

    wf_id = _identify_workflow_id(metadata) if metadata else workflow_id
    costs = estimate_workflow_cost(
        metadata,
        workflow_id,
        sku_list,
        host,
        port,
        tier_scheme
    )

    if report == 'raw':
        creport.raw_cost_report(wf_id, costs)
    else:
        creport.standard_cost_report(wf_id, costs, nanos)

@cli.command(short_help="get workflow status")
@click.option('--host', type=click.STRING, default='localhost',
              help='cromwell web server host')
@click.option('--port', type=click.INT, default=8000,
              help='cromwell web server port')
@click.argument('workflow-id', type=click.STRING)
def status(workflow_id, host, port):
    server = cromwell.Server(host, port)
    if not server.is_accessible():
        msg = "Could not access the cromwell server!  Please ensure it is up!"
        raise Exception(msg)
    status = server.get_workflow_status(workflow_id)
    creport.display_workflow_status(workflow_id, status)

@cli.command(name='execution-status', short_help="get workflow execution status")
@click.option('--host', type=click.STRING, default='localhost',
        help='cromwell web server host')
@click.option('--port', type=click.INT, default=8000,
        help='cromwell web server port')
@click.argument('workflow-id', type=click.STRING)
def execution_status(workflow_id, host, port):
    server = cromwell.Server(host, port)
    if not server.is_accessible():
        msg = "Could not access the cromwell server!  Please ensure it is up!"
        raise Exception(msg)
    status_summary = server.get_workflow_execution_status(workflow_id)
    creport.display_workflow_execution_status(workflow_id, status_summary)

@cli.command(short_help="metadata on inputs, outputs and status")
@click.option('--host', type=click.STRING, default='localhost',
              help='cromwell web server host')
@click.option('--port', type=click.INT, default=8000,
              help='cromwell web server port')
@click.argument('workflow-id', type=click.STRING)
def outputs(workflow_id, host, port):
    server = cromwell.Server(host, port)
    if not server.is_accessible():
        msg = "Could not access the cromwell server!  Please ensure it is up!"
        raise Exception(msg)
    metadata = server.get_workflow_input_outputs(workflow_id)
    pretty_metadata = json.dumps(metadata, indent=4, sort_keys=True)
    print(pretty_metadata)

@cli.command(short_help="abort workflow")
@click.option('--host', type=click.STRING, default='localhost',
              help='cromwell web server host')
@click.option('--port', type=click.INT, default=8000,
              help='cromwell web server port')
@click.argument('workflow-id', type=click.STRING)
def abort(workflow_id, host, port):
    server = cromwell.Server(host, port)
    if not server.is_accessible():
        msg = "Could not access the cromwell server!  Please ensure it is up!"
        raise Exception(msg)
    status = server.abort_workflow(workflow_id)
    print(json.dumps(status, indent=4, sort_keys=True))

@cli.command(short_help="Inspect billing via BigQuery")
def bq():
    sys.exit('[err] Subcommand not implemented yet!')

@cli.command(short_help="generate a workflow report", name="wf")
@click.option('--metadata', 'metadata_path', type=click.Path(exists=True), default=None,
              help=('Path to an existing (not-raw) '
                    'cromwell workflow metadata json file.'))
@click.option('--workflow-id', type=click.STRING, default=None,
              help=('A cromwell workflow-id to fetch metadata from '
                    'the cromwell server'))
@click.option('--host', type=click.STRING, default='localhost',
              help='cromwell web server host')
@click.option('--port', type=click.INT, default=8000,
              help='cromwell web server port')
@click.option('--report', type=click.Choice(creport.workflow_report_types()),
              default='summary',
              help='output report choices')
@click.option('--opts', type=click.STRING, default=None,
              help='specialized report options')
@click.option('-v', '--verbose', count=True,
              help='verbosity level')
def wf(metadata_path,
       workflow_id,
       host,
       port,
       report,
       opts,
       verbose):
    if verbose:
        _setup_logging_level(verbose)

    # otherwise prepare to cost calcuate and then report
    if (metadata_path is None) and (workflow_id is None):
        sys.exit(("[err] Please specify either a "
                  "'--metadata' or '--workflow-id' option!"))

    metadata = _get_metadata_json(
        metadata_path=metadata_path,
        workflow_id=workflow_id,
        host=host,
        port=port
    )

    creport.workflow_report(report, metadata, opts)

# -- Helper functions ----------------------------------------------------------
def _identify_workflow_id(metadata_json):
    wf_id = None
    with open(metadata_json, 'r') as f:
        data = json.load(f)
        wf_id = data['id']
    return wf_id

def _setup_logging_level(verbosity_level):
    if verbosity_level == 1:
        logging.getLogger('root').setLevel(logging.DEBUG)

    if verbosity_level == 2:
        logging.getLogger('root').setLevel(logging.DEBUG)
        _enable_third_party_module_logs(logging.INFO)

    if verbosity_level >= 3:
        logging.getLogger('root').setLevel(logging.DEBUG)
        _enable_third_party_module_logs(logging.DEBUG)

def _enable_third_party_module_logs(level):
    # re-enable the loggers from the other third-party modules
    for name in logging.Logger.manager.loggerDict.keys():
        logging.getLogger(name).setLevel(level)

def _get_metadata_json(metadata_path=None, workflow_id=None,
                       host='localhost', port=8000):
    metadata = None
    if metadata_path:
        msg = "Loading the workflow metadata from : {}".format(metadata_path)
        logging.info(msg)
        with open(metadata_path) as f:
            metadata = json.load(f)
    else:
        logging.info("Fetching metadata from cromwell")
        server = _get_cromwell_server(host, port)
        metadata = server.get_workflow_metadata(workflow_id)
        logging.info("Fetched metadata from cromwell")

    if metadata is None:
        msg = "Could not derive workflow metadata"
        logger.error(msg)
        raise Exception(msg)

    return metadata

def _get_cromwell_server(host, port):
    # setup the server object
    # decorate the cromwell.Server class function
    cromwell.Server.get_workflow_metadata = \
        utils.memoize(cromwell.Server.get_workflow_metadata)
    server = cromwell.Server(host, port)

    logging.info("Checking if we have access to the cromwell server")
    if not server.is_accessible():
        msg = "Could not access the cromwell server!  Please ensure it is up!"
        logging.error(msg)
        raise Exception(msg)

    return server

def estimate_workflow_cost(metadata_path=None,
                           workflow_id=None,
                           sku_path=None,
                           host='localhost',
                           port=8000,
                           tier_scheme='all'):
    # setup the server object
    # decorate the cromwell.Server class function
    cromwell.Server.get_workflow_metadata = \
        utils.memoize(cromwell.Server.get_workflow_metadata)
    server = cromwell.Server(host, port)

    logging.info("Checking if we have access to the cromwell server")
    if not server.is_accessible():
        msg = "Could not access the cromwell server!  Please ensure it is up!"
        logging.error(msg)
        raise Exception(msg)

    # setup the google services and skus information
    gcloud.GoogleServices.get_available_compute_types = \
        utils.memoize(gcloud.GoogleServices.get_available_compute_types)
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
    cost = estimator.calculate_cost(metadata, tier_scheme)
    logging.info("Finished cost calculations")

    return cost
