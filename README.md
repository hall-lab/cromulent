
# cromulent 

A [cromulent][0] (_[watch][3]_) assistant for [cromwell workflows][1] run on the cloud (_currently only [Google Cloud Platform][2]_).

## Features

* Estimate cost of a cromwell workflow. (_Doesn't include network egress or sustained usage discounts. Not all resource types included._)
    
    Cost is calculated by pricing the cpu, memory and disk usage of each [Google Genomics Operation][7] present in the cromwell metadata. The idea is based on comments made [in the GATK Forum][4].

* Quickly get workflow statuses
* Easily retrieve current Google Compute Engine &amp; Persistent Disk Costs via the [Google Cloud Billing API][6]
* Easily retrieve workflow metadata from the cromwell server



## Requirements

* python 2.7
* Google Cloud Account
* [gcloud][5]

## Installation

    pip install https://github.com/hall-lab/cromulent

Additionally, you may need to authorize application default credentials via `gcloud` before running cromulent

    gcloud auth application-default login

### Installation for Developing on `cromulent`

    gcloud auth application-default login
    git clone https://github.com/hall-lab/cromulent
    cd cromulent
    virtualenv venv
    source venv/bin/activate
    pip install -e .

## Usage

The main interface is the `cromulent` terminal command.  It has a git-like sub-command interface.

Try typing `cromulent --help` on the command line and see what options are available.

    Usage: cromulent [OPTIONS] COMMAND [ARGS]...
    
      A collection of cromwell helpers.
    
    Options:
      --version   Show the version and exit.
      -h, --help  Show this message and exit.
    
    Commands:
      abort             abort workflow
      bq                Inspect billing via BigQuery
      estimate          estimate ideal workflow cost
      execution-status  get workflow execution status
      metadata          retrieve metadata for workflow-id
      outputs           metadata on inputs, outputs and status
      sku-list          retrieve sku pricing info from the Google Cloud API
      status            get workflow status
      wf                generate a workflow report

Each subcommand will have it own set of options.  Try `cromulent <subcommand> --help` for more details on each subcommand.

## Workflow Reports

The `cromwell wf` subcommand contains various report types.

The `cromulent wf` subcommand takes an `--opts` parameter that is specialized for a given `--report` option.  The `--opts` parameter must be in the following key/value command separated format:

    --opts='key1=value1;key2=value2;...'

The `opts` parameter can be used on certain report types.  See the documentation and examples below.

### Summary Report

This is a report to get an overall status of where things are with a given workflow.  _For large cromwell workflows it is probably convenient to cache the workflow metadata via the `cromulent metadata` command._

### Command

    $ cromulent wf --metadata 45a3953a-052e-4aca-a3f1-51d313e01d99.json --report=summary

or
    $ cromulent wf --workflow-id 45a3953a-052e-4aca-a3f1-51d313e01d99 --report=summary

### Example Output

    $ cromulent wf --metadata 45a3953a-052e-4aca-a3f1-51d313e01d99.json --report=summary
    [2018-11-22 17:00:12,967] : root : INFO : Loading the workflow metadata from : 45a3953a-052e-4aca-a3f1-51d313e01d99.json
    
    ID         : 45a3953a-052e-4aca-a3f1-51d313e01d99
    Status     : Failed
    Submit Time: 2018-11-21T21:53:29.101Z (UTC)
    Start  Time: 2018-11-21T21:53:32.826Z (UTC)
    End    Time: 2018-11-22T18:51:48.954Z (UTC)
    
    call                                             Done    Failed    RetryableFailure
    ---------------------------------------------  ------  --------  ------------------
    JointGenotyping.ImportGVCFs                       719      9468                 827
    JointGenotyping.DynamicallyCombineIntervals         1         0                   0
    JointGenotyping.GenotypeGVCFs                     719         0                   9
    JointGenotyping.HardFilterAndMakeSitesOnlyVcf     719         0                   1
    JointGenotyping.CollectGVCFs                    10187         0                   0

## Failures Summary Report

### Command

    $ cromulent wf --report=failures --metadata=45a3953a-052e-4aca-a3f1-51d313e01d99.json --opts='detail=true'
    $ cromulent wf --report=failures --metadata=45a3953a-052e-4aca-a3f1-51d313e01d99.json --opts='detail=true;calls=JointGenotyping.ImportGVCFs'
    $ cromulent wf --report=failures --metadata=45a3953a-052e-4aca-a3f1-51d313e01d99.json

### Example Output

Simple failure report output:

     $ cromulent wf --report=failures --metadata=45a3953a-052e-4aca-a3f1-51d313e01d99.json  | head -n 5
     [2018-11-22 17:23:28,143] : root : INFO : Loading the workflow metadata from : 45a3953a-052e-4aca-a3f1-51d313e01d99.json
     
     call                           shard  jobId                                                                     rc  stderr
     ---------------------------  -------  ----------------------------------------------------------------------  ----  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     JointGenotyping.ImportGVCFs        0  projects/washu-genome-inh-dis-analysis/operations/8797592820173599617      1  gs://wustl-ccdg-costa-rican-callset-2018-11/cromwell/cromwell-executions/JointGenotyping/45a3953a-052e-4aca-a3f1-51d313e01d99/call-ImportGVCFs/shard-0/stderr
     JointGenotyping.ImportGVCFs        1  projects/washu-genome-inh-dis-analysis/operations/12240229225160402087     1  gs://wustl-ccdg-costa-rican-callset-2018-11/cromwell/cromwell-executions/JointGenotyping/45a3953a-052e-4aca-a3f1-51d313e01d99/call-ImportGVCFs/shard-1/stderr

Detailed failure report output:

    $ cromulent wf --report=failures --metadata=45a3953a-052e-4aca-a3f1-51d313e01d99.json --opts='detail=true' | head -n 60
    [2018-11-22 17:18:14,064] : root : INFO : Loading the workflow metadata from : 45a3953a-052e-4aca-a3f1-51d313e01d99.json
    
    call: JointGenotyping.ImportGVCFs | shard: 0 | jobId: projects/my-google-project-id/operations/8797592820173599617 | rc: 1
        --- Error Message: ---
    
        | Task JointGenotyping.ImportGVCFs:0:1 failed. Job exit code 1. Check gs://wustl-callset-bucket/cromwell/cromwell-executions/JointGenotyping/45a3953a-052e-4aca-a3f1-51d313e01d99/call-ImportGVCFs/shard-0/stderr for more information. PAPI error code 9. Execution failed: action 9: unexpected exit status 1 was not ignored
        | [Delocalization] Unexpected exit status 1 while running "/bin/sh -c retry() { for i in `seq 3`; do gsutil   cp /cromwell_root/genomicsdb.tar gs://wustl-callset-bucket/cromwell/cromwell-executions/JointGenotyping/45a3953a-052e-4aca-a3f1-51d313e01d99/call-ImportGVCFs/shard-0/ 2> gsutil_output.txt; RC_GSUTIL=$?; if [[ \"$RC_GSUTIL\" -eq 1 ]]; then\n grep \"Bucket is requester pays bucket but no user project provided.\" gsutil_output.txt && echo \"Retrying with user project\"; gsutil -u my-google-project-id  cp /cromwell_root/genomicsdb.tar gs://wustl-callset-bucket/cromwell/cromwell-executions/JointGenotyping/45a3953a-052e-4aca-a3f1-51d313e01d99/call-ImportGVCFs/shard-0/; fi ; RC=$?; if [[ \"$RC\" -eq 0 ]]; then break; fi; sleep 5; done; return \"$RC\"; }; retry": CommandException: No URLs matched: /cromwell_root/genomicsdb.tar
        | CommandException: No URLs matched: /cromwell_root/genomicsdb.tar
        | CommandException: No URLs matched: /cromwell_root/genomicsdb.tar
        |
    
        --- Inputs: ---
    
          {
              "batch_size": 50,
              "disk_size": 200,
              "docker": "us.gcr.io/my-google-project-id/gatk-4:4.0.6.0",
              "gatk_path": "/gatk/gatk",
              "interval": "chr1:1-391754",
              "sample_name_map": "gs://wustl-callset-bucket/cromwell/cromwell-executions/JointGenotyping/45a3953a-052e-4aca-a3f1-51d313e01d99/call-CollectGVCFs/shard-0/gvcf.sample_map",
              "workspace_dir_name": "genomicsdb"
          }
    
        --- stderr: ---
    
          gs://wustl-callset-bucket/cromwell/cromwell-executions/JointGenotyping/45a3953a-052e-4aca-a3f1-51d313e01d99/call-ImportGVCFs/shard-0/stderr
    
        --- jes: ---
    
          {
              "endpointUrl": "https://genomics.googleapis.com/",
              "executionBucket": "gs://wustl-callset-bucket/cromwell/cromwell-executions",
              "googleProject": "my-google-project-id",
              "instanceName": "google-pipelines-worker-25baaac3f73cd72fd20af6a00fb7d438",
              "machineType": "custom-2-6912",
              "monitoringScript": "gs://wustl-monitoring-bucket/mem_monitor.sh",
              "zone": "us-central1-f"
          }
    
        --- runtime: ---
    
          {
              "bootDiskSizeGb": "10",
              "continueOnReturnCode": "0",
              "cpu": "2",
              "cpuMin": "1",
              "disks": "local-disk 200 HDD",
              "docker": "us.gcr.io/my-google-project-id/gatk-4:4.0.6.0",
              "failOnStderr": "false",
              "maxRetries": "0",
              "memory": "7000.0 MB",
              "memoryMin": "2048.0 MB",
              "noAddress": "false",
              "preemptible": "5",
              "zones": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
          }
    
    
    call: JointGenotyping.ImportGVCFs | shard: 1 | jobId: projects/my-google-project-id/operations/12240229225160402087 | rc: 1
        --- Error Message: --- 
        ...

#### Specialized Options

These are options that be used on the `--opts` option parameter.

* `detail=true`
    
    This will turn on the detailed failure report format.  It will contain error message, input, output and execution details for a given job.

* `calls=wf.call_name`

    For example `calls=JointGenotyping.ImportGVCFs` will only produce the detail report for the tasks involved in the `JointGenotyping.ImportGVCFs` call of the corresponding workflow WDL file.

The following

**NOTE:** _This sofware is currently in alpha stage development, and is continously changing.  Newer subcommands and features are currently in development._

[0]: https://en.oxforddictionaries.com/definition/cromulent
[1]: https://github.com/broadinstitute/cromwell
[2]: https://cloud.google.com
[3]: https://www.youtube.com/watch?v=QPR1stojkWA
[4]: https://gatkforums.broadinstitute.org/firecloud/discussion/9130/cromwell-polling-interval-is-sometimes-too-long
[5]: https://cloud.google.com/pubsub/docs/quickstart-cli
[6]: https://cloud.google.com/billing/docs/apis
[7]: https://cloud.google.com/genomics/reference/rest/Shared.Types/ListOperationsResponse#Operation
