from __future__ import division
import dateutil.parser
import math, os, sys
from collections import namedtuple

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

class Disk(object):

    def __init__(self, size, disk_type='PERSISTENT_HDD'):
        self.size = size
        self.type_ = disk_type


class GenomicsOperation(object):

    def __init__(self, response_json):
        meta = response_json['metadata']
        gce = meta['runtimeMetadata']['computeEngine']
        self.machine = gce['machineType'].split('/')[1]
        self.zone = gce['zone']
        self.region, _ = self.zone.rsplit('-', 1)
        resources_dict = meta['request']['pipelineArgs']['resources']
        self.preemptible = resources_dict['preemptible']
        self.start_time = dateutil.parser.parse(
            meta['startTime']
            )

        try:
            self.end_time = dateutil.parser.parse(meta['endTime'])
            self.length = self.end_time - self.start_time
        except KeyError:
            self.end_time = None
            self.length = None

        self.disks = [
            Disk(x['sizeGb'], x['type']) for x in resources_dict['disks']
            ]
        self.disks.append(Disk(resources_dict['bootDiskSizeGb']))

    def duration(self):
        if self.length:
            return self.length.total_seconds()
        else:
            return None

    def __str__(self):
        return ("(machine: {}, "
                "zone: {}, "
                "region: {}, "
                "preemptible: {}, "
                "start_time: {}, "
                "end_time: {}, "
                "length: {}, "
                "duration: {} )").format(
                        self.machine,
                        self.zone,
                        self.region,
                        self.preemptible,
                        self.start_time,
                        self.end_time,
                        self.length,
                        self.duration()
                        )


Resource = namedtuple('Resource', ['duration', 'region', 'name', 'units'])


def vm_resource_name(name, premptible):
    identifier = 'CP-COMPUTEENGINE-VMIMAGE-{0}'.format(name.upper())
    if premptible:
        identifier = identifier + '-PREEMPTIBLE'
    return identifier


def disk_resource_name(type_):
    lineitem = 'CP-COMPUTEENGINE-STORAGE-PD-{0}'
    if type_ == 'PERSISTENT_HDD':
        disk_code = 'CAPACITY'
    elif type_ == 'PERSISTENT_SSD':
        disk_code = 'SSD'
    else:
        raise RuntimeError('Unknown disk type: {0}'.format(type_))
    return lineitem.format(disk_code)


def vm_duration(duration):
    '''Return the duration in hours with a minimum of 10 minutes.'''
    # All machine types are charged a minimum of 10 minutes.
    # See https://cloud.google.com/compute/pricing#billingmodel
    minutes = duration / 60.0
    if minutes < 10:  # Enforce minimum of 10 minutes
        price_duration = 10
    else:
        price_duration = math.ceil(minutes)  # round up to nearest minute
    return price_duration / 60  # convert to hours to match price


def disk_lifetime(duration):
    '''Return the duration in months based on a 30.5 day month.'''
    # convert to months. Assuming a 30.5 day month or 732 hours based on
    # footnote here: https://cloud.google.com/compute/pricing#localssdpricing
    # rounding up the seconds based on https://cloud.google.com/compute/pricing#disk.
    # Not sure if necessary depending on how timing is done in operations
    # NOTE A 730 hour month may be more appropriate based on the above footnote
    # and https://cloud.google.com/compute/pricing#billingmodel
    return math.ceil(duration) / 60 / 60 / 24 / 30.5


def vm_resource(op):
    return Resource(
            duration=vm_duration(op.duration()),
            region=op.region,
            name=vm_resource_name(op.machine, op.preemptible),
            units=1,
            )


def disk_resources(op):
    return [Resource(
                duration=disk_lifetime(op.duration()),
                region=op.region,
                name=disk_resource_name(d.type_),
                units=d.size,
                ) for d in op.disks
            ]


def as_resources(op):
    resources = disk_resources(op)
    resources.append(vm_resource(op))
    return resources


class OperationCostCalculator(object):

    def __init__(self, pricelist_json):
        self.pricelist_json = pricelist_json

    def cost(self, operation):
        return sum([self.resource_cost(x) for x in as_resources(operation)])

    def price(self, resource):
        return self.pricelist_json['gcp_price_list'][resource.name][resource.region]

    def resource_cost(self, resource):
        return resource.duration * self.price(resource) * resource.units

# functions to gather the latest Google Cloud Platform Compute costs

# loosely based on https://github.com/google/google-api-python-client/issues/484
# and https://gist.github.com/indraniel/cc3c4d1c5f03ba05bcc7793c7d166338
# and https://developers.google.com/resources/api-libraries/documentation/cloudbilling/v1/python/latest/cloudbilling_v1.services.skus.html
# and https://cloud.google.com/billing/reference/rest/v1/services.skus/list
def setup_cloudbilling_api_access():
    if 'GCP_API_KEY' not in os.environ:
        msg = ("Please supply the shell environment variable: 'GCP_API_KEY' "
               "before running this script!\n"
               "(e.g. 'export GCP_API_KEY=\"your api key\"')")
        sys.exit(msg)
    api_key = os.environ['GCP_API_KEY']
    cloudbilling = discovery.build('cloudbilling', 'v1', developerKey=api_key)
    return cloudbilling

def get_service_id(billing_api, service_name):
    response = billing_api.services().list().execute()
    compute_service = filter(lambda(x): x['displayName'] == service_name, response['services'])
    if not compute_service:
        raise("Didn't find '{}' in billing API service list".format(service_name))
    return compute_service[0]

def machine_types():
    compute_types = ('standard', 'highmem', 'highcpu')
    compute_cores = (2, 4, 8, 16, 32, 64)

    computes = {}
    for t in compute_types:
        for c in compute_cores:
            formal_name = "{} Intel N1 {} VCPU running in Americas".format(t.capitalize(), c)
            short_name = "n1-{}-{}".format(t, c)
            computes[short_name] = formal_name

    # add special cases
    computes['n1-standard-1'] = "Standard Intel N1 1 VCPU running in Americas"
    computes['f1-micro'] = "Micro instance with burstable CPU running in Americas"
    computes['g1-small'] = "Small instance with 1 VCPU running in Americas"

    return computes

def get_compute_price(sku):
    nano_price = sku['pricingInfo'][0]['pricingExpression']['tieredRates'][0]['unitPrice']['nanos']
    price = nano_price * 1e-9
    return price

def get_skus_for_service(billing_api, service_info):
    service_name = service_info["name"]
    response = billing_api.services().skus().list(parent=service_name).execute()

    # assemble the skus into a better format
    service_skus = {}
    while True:
        for sku in response['skus']:
            description = sku['description']
            service_skus[description] = sku
        if response['nextPageToken']:
            response = billing_api.services().skus().list(parent=service_name, pageToken=response['nextPageToken']).execute()
        else:
            break

    return service_skus

def construct_pricelist(billing_api, service_info):
    compute_skus = get_skus_for_service(billing_api, service_info)

    # get the machine types we really care about (mostly standard compute in the USA)
    machines = machine_types()

    pricelist = {}
    for i in machines:
        short_name = i
        formal_name = machines[short_name]
        preemptible_name = "Preemptible {}".format(formal_name)
        normal_sku = compute_skus[formal_name]
        preemptible_sku = compute_skus[preemptible_name]

        pricelist[short_name] = {
            'description' : normal_sku["description"],
            'normal' : {
                'skuId' : normal_sku['skuId'],
                'price' : get_compute_price(normal_sku),
            },
            'preemptible' : {
                'skuId' : preemptible_sku['skuId'],
                'price' : get_compute_price(preemptible_sku),
            },
        }

    return pricelist

def generate_gcp_compute_pricelist():
    cloudbilling = setup_cloudbilling_api_access()
    compute_service = get_service_id(cloudbilling, 'Compute Engine')
    pricelist = construct_pricelist(cloudbilling, compute_service)
    return pricelist
