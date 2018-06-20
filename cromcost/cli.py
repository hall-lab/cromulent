from __future__ import print_function

import signal, sys, os, json

import click

from cromcost.version import __version__
import cromcost.cromwell as cromwell
import cromcost.calculate as calc
import cromcost.report as report

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__)
def cli():
    '''A collection of cromwell helpers to estimate cloud costs'''
    # to make this script/module behave nicely with unix pipes
    # http://newbebweb.blogspot.com/2012/02/python-head-ioerror-errno-32-broken.html
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

@cli.command(name='price-list', short_help="pricing info")
@click.option('--raw', is_flag=True,
              help='dump the raw compute engine sku prices')
@click.option('--output', type=click.Path(), default=None,
              help='Path to dump the raw JSON pricing information to')
def price_list(output, raw):
    if raw:
        import cromcost.gcloud as gcloud
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
@click.option('--metadata', type=click.Path(),
              help='Path to a fully formed cromwell metadata json file.')
@click.option('--workflow-id', type=click.STRING,
              help=('A cromwell workflow-id to fetch metadata from '
                    'the cromwell server'))
@click.option('--host', type=click.STRING, default='localhost',
              help='cromwell web server host')
@click.option('--port', type=click.INT, default=8000,
              help='cromwell web server port')
def estimate(metadata, workflow_id, host, port):
    costs = calc.ideal_workflow_cost(metadata, workflow_id, host, port)
    report.standard_cost_report(costs)

@cli.command(short_help="Inspect billing via BigQuery")
def bq():
    sys.exit('[err] Subcommand not implemented yet!')
