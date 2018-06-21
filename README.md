
# cromulent 

A [cromulent][0] (_[watch][3]_) assistant for [cromwell workflows][1] run on the cloud (_currently only [Google Cloud Platform][2]_).

## Features

* Estimate cost of cromwell workflow on Google. (_Doesn't include network egress or sustained usage discounts. Not all resource types included._)
    
    Resource usage is calculated by querying the genomics api using operations ids present in the cromwell metadata. The idea is based on comments made [in the GATK Forum][4]. 

* Quickly get workflow statuses
* Easily retrive current Google Compute Engine &amp; Persistent Disk Costs via the [Google Cloud Billing API][6]



## Requirements

* python 2.7
* Google Cloud Account
* [gcloud][5]

## Installation

    pip install https://github.com/ernfrid/cromulent

Additionally, you may need to authorize application default credentials via `gcloud` before running cromulent

    gcloud auth application-default login

## Usage

The main interface is the `cromulent` terminal command.  It has a git-like sub-command interface.

Try typing `cromulent --help` on the command line and see what options are available.


    Usage: cromulent [OPTIONS] COMMAND [ARGS]...
    
      A collection of cromwell helpers to estimate cloud costs
    
    Options:
      --version   Show the version and exit.
      -h, --help  Show this message and exit.
    
    Commands:
      bq          Inspect billing via BigQuery
      estimate    estimate ideal workflow cost
      metadata    retrieve metadata for workflow-id
      price-list  retrieve pricing info from the Google Cloud API
      status      get workflow status

Each subcommand will have it own set of options.  Try `cromulent <subcommand> --help` for more details on each subcommand.

**NOTE:** _This sofware is currently in alpha stage development, and is continously changing.  Newer subcommands and features are currently in development._

[0]: https://en.oxforddictionaries.com/definition/cromulent
[1]: https://github.com/broadinstitute/cromwell
[2]: https://cloud.google.com
[3]: https://www.youtube.com/watch?v=QPR1stojkWA
[4]: https://gatkforums.broadinstitute.org/firecloud/discussion/9130/cromwell-polling-interval-is-sometimes-too-long
[5]: https://cloud.google.com/pubsub/docs/quickstart-cli
[6]: https://cloud.google.com/billing/docs/apis
