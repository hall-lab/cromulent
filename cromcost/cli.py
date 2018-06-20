from __future__ import print_function

import signal, sys, os, json, logging

import click

from cromcost.version import __version__
import cromcost.cromwell as cromwell
import cromcost.calculate as calc
import cromcost.gcloud as gcloud
import cromcost.report as creport

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
    '''A collection of cromwell helpers to estimate cloud costs'''
    # to make this script/module behave nicely with unix pipes
    # http://newbebweb.blogspot.com/2012/02/python-head-ioerror-errno-32-broken.html
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

# -- Subcommands ---------------------------------------------------------------
@cli.command(name='price-list', short_help="pricing info")
@click.option('--raw', is_flag=True,
              help='dump the raw compute engine sku prices')
@click.option('--output', type=click.Path(), default=None,
              help='Path to dump the raw JSON pricing information to')
def price_list(output, raw):
    if raw:
        data = gcloud.get_raw_compute_engine_skus()
    else:
        data = calc.generate_gcp_compute_pricelist()
    pricelist = json.dumps(data, indent=4, sort_keys=True)
    if output:
        with open(output, 'w') as f:
            print(pricelist, file=f)
    else:
        print(pricelist)

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
        calc.memoize(cromwell.Server.get_workflow_metadata)
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
@click.option('--price-list', type=click.Path(exists=True), default=None,
              help='Path to an existing pricelist json file.')
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
@click.option('--report', type=click.Choice(['standard', 'raw']),
              default='standard',
              help='output report choice')
@click.option('-v', '--verbose', count=True,
              help='verbosity level')
def estimate(metadata,
             price_list,
             import_raw_cost_data,
             workflow_id,
             host,
             port,
             report,
             verbose):
    if verbose:
        _setup_logging_level(verbose)

    reporter = {
        'raw' : creport.raw_cost_report,
        'standard' : creport.standard_cost_report
    }

    report_fn = reporter[report]

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
    costs = calc.ideal_workflow_cost(
        metadata,
        workflow_id,
        price_list,
        host,
        port
    )

    report_fn(wf_id, costs)

@cli.command(short_help="Inspect billing via BigQuery")
def bq():
    sys.exit('[err] Subcommand not implemented yet!')

# -- Helper functions ----------------------------------------------------------
def _identify_workflow_id(metadata_json):
    wf_id = None
    with open(metadata, 'r') as f:
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
