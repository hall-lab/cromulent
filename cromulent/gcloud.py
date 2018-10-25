from __future__ import division

import dateutil.parser
import math, os, sys
import logging
from collections import namedtuple

from googleapiclient import discovery
import google.auth

class Resource(object):

    def __init__(self, duration):
        self.duration = duration # in seconds

    def google_pricing_duration(self):
        # All cpu and memory related charges are prorated on seconds
        # All machine types (memory and cores) are charged a minimum of 1 minute (60 seconds).
        # See https://cloud.google.com/compute/pricing#billingmodel
        if self.duration < 60:  # Enforce minimum of 1 minute (60 seconds)
            return 60

        return self.duration

    def get_unit_price(self, sku):
        # TODO:  handle the multiple tired rate case
        # first-pass -- NOT handling multiple tiered rates at the moment
        return int(sku['pricingInfo']['pricingExpression']['tieredRates'][0]['unitPrice']['nanos'])

    def get_base_unit_conversion_factor(self, sku):
        return float(sku['pricingInfo']['pricingExpression']['baseUnitConversionFactor'])

class Disk(Resource):

    def __init__(self, size, duration=duration, disk_type='pd-standard'):
        self.size = size # in gb
        self.type_ = disk_type
        super(Resource, self).__init__(**kwargs)

    def disk_label(self):
        return self.type_

    # All disk-related charges are prorated on seconds
    def compute_nano_dollars(sku):
        base_price = self.get_base_price(sku) # nano dollars / (byte * second)
        bytes_ = self.size * 1024.0 * 1024.0 * 1024.0
        seconds = self.duration
        base_unit_usage = bytes_ * seconds
        nano_dollars = base_unit_usage * base_price
        return nano_dollars

    def get_base_price(self, sku):
        # unit price is in nano dollars / (GiB * month)
        unit_price = self.get_unit_price(sku)
        # base price is in nano dollars / (byte * second)
        base_price = unit_price / self.get_base_unit_conversion_factor(sku)
        return base_price


class Cpu(Resource):

    def __init__(self, cores, duration=duration):
        self.cores = cores
        super(Resource, self).__init__(**kwargs)

    # All cpu charges are prorated on seconds with min being 60 secs
    def compute_nano_dollars(sku):
        base_price = self.get_base_price(sku) # nano dollars / (second)
        seconds = self.google_pricing_duration()
        base_unit_usage = seconds
        nano_dollars = base_unit_usage * base_price * self.cores
        return nano_dollars

    def get_base_price(self, sku):
        # unit price is in nano dollars / (hour)
        unit_price = self.get_unit_price(sku)
        # base price is in nano dollars / (second)
        base_price = unit_price / self.get_base_unit_conversion_factor(sku)
        return base_price

class Ram(Resource):

    def __init__(self, size, duration=duration):
        self.size = size # in gb
        self.type_ = disk_type
        super(Resource, self).__init__(**kwargs)

    # All memory charges are prorated on seconds with min being 60 secs
    def compute_nano_dollars(sku):
        base_price = self.get_base_price(sku) # nano dollars / (second)
        bytes_ = self.size * 1024.0 * 1024.0 * 1024.0
        seconds = self.google_pricing_duration()
        base_unit_usage = bytes_ * seconds
        nano_dollars = base_unit_usage * base_price
        return nano_dollars

    def get_base_price(self, sku):
        # unit price is in nano dollars / (GiB * hour)
        unit_price = self.get_unit_price(sku)
        # base price is in nano dollars / (byte * second)
        base_price = unit_price / self.get_base_unit_conversion_factor(sku)
        return base_price


class GenomicsOperation(object):

    def __init__(self, response_json):
        meta = response_json['metadata']
        vm =  meta['pipeline']['resources']['virtualMachine']
        # This is now likely to be something like custom-8-7424, even for pre-defined types.
        self.machine = vm['machineType']
        # TODO - is this ok to always take the earliest event to get zone? Is it always VM starting?
        self.zone = meta['events'][-1]['details']['zone']
        self.region, _ = self.zone.rsplit('-', 1)
        self.preemptible = vm['preemptible']
        self.project = meta['pipeline']['resources']['projectId']
        self.start_time = dateutil.parser.parse(meta['startTime'])

        try:
            self.end_time = dateutil.parser.parse(meta['endTime'])
            self.length = self.end_time - self.start_time
        except KeyError:
            self.end_time = None
            self.length = None

        time_elapsed = self.duration()

        # setup the cpu and ram components
        _, cpus, mem_mb = self.machine.split('-')
        mem_gb = mem_mb / 1024.0
        self.cpu = Cpu(cores=cpus, duration=time_elapsed)
        self.ram = Ram(size=mem_gb, duration=self.time_elapsed)

        # setup the disk components
        self.disks = [ Disk(x['sizeGb'], x['type'], duration=time_elapsed)
                       for x in vm['disks'] ]
        self.disks.append(Disk(vm['bootDiskSizeGb'], duration=time_elapsed))

    def duration(self):
        if self.length:
            return self.length.total_seconds()
        else:
            return None

    def __str__(self):
        return ("(machine: {}, "
                "disks: {}, "
                "zone: {}, "
                "region: {}, "
                "preemptible: {}, "
                "project: {},"
                "start_time: {}, "
                "end_time: {}, "
                "length: {}, "
                "duration: {} )").format(
                        self.machine,
                        self.disks,
                        self.zone,
                        self.region,
                        self.preemptible,
                        self.project,
                        self.start_time,
                        self.end_time,
                        self.length,
                        self.duration()
                        )


class GoogleServices(object):

    # loosely based on https://github.com/google/google-api-python-client/issues/484
    # and https://gist.github.com/indraniel/cc3c4d1c5f03ba05bcc7793c7d166338
    # and https://developers.google.com/resources/api-libraries/documentation/cloudbilling/v1/python/latest/cloudbilling_v1.services.skus.html
    # and https://cloud.google.com/billing/reference/rest/v1/services.skus/list
    # and https://cloud.google.com/compute/pricing#disk
    def __init__(self, sku_path=None):
        self._ensure_environment()
        api_key = os.environ['GCP_API_KEY']

        credentials, project = google.auth.default()
        self.credentials = credentials
        self.project = project

        self.billing  = discovery.build('cloudbilling', 'v1', developerKey=api_key)
        self.compute  = discovery.build('compute', 'v1', credentials=credentials)
        self.genomics = discovery.build('genomics', 'v2alpha1', credentials=credentials)

        self.sku_list = self._construct_compute_sku_list(sku_path)

    def _ensure_environment(self):
        if 'GCP_API_KEY' not in os.environ:
            msg = ("Please supply the shell environment variable: 'GCP_API_KEY' "
                   "before running this script!\n"
                   "(e.g. 'export GCP_API_KEY=\"your api key\"')")
            sys.exit(msg)

    def compute_engine_skus(self):
        return self.sku_list

    def _construct_compute_sku_list(self, sku_path):
        sku_data = None
        if sku_path is None:
            logging.info("Obtaining the compute price list from Google Cloud")
            sku_data = self._get_raw_compute_engine_skus()
        else:
            logging.info("Obtaining the compute price list from {}".format(sku_path))
            with open(sku_path, 'r') as f:
                sku_data = json.load(f)

        return sku_data

    def _get_raw_compute_engine_skus(self):
        compute_service = self._get_billing_service('Compute Engine')
        compute_skus = self._get_billing_skus_for_service(compute_service)
        return compute_skus

    def _get_billing_service(self, service_name):
        response = self.billing.services().list().execute()
        compute_service = filter(lambda(x): x['displayName'] == service_name, response['services'])
        if not compute_service:
            raise("Didn't find '{}' in billing API service list".format(service_name))
        return compute_service[0]

    def _get_billing_skus_for_service(self, service_info):
        service_name = service_info["name"]
        response = self.billing.services().skus().list(parent=service_name).execute()

        # assemble the skus into a better format
        service_skus = {}
        while True:
            for sku in response['skus']:
                description = sku['description']
                service_skus[description] = sku
            if response['nextPageToken']:
                response = self.billing_api.services().skus().list(parent=service_name, pageToken=response['nextPageToken']).execute()
            else:
                break

        return service_skus

    def get_genomics_operation_metadata(self, name):
        request = self.genomics.projects().operations().get(name=name)
        response = request.execute()
        return response

    def estimate_genomics_operation_cost(self, operation):
        # a genomics operation cost consists of 3 parts/resources:
        #    1.  cpu/core usage
        #    2.  memory usage
        #    3.  disk usage
        # we need to calculate the cost of each resource and then add up the total
        core_sku = self.identify_google_compute_sku(operation, 'Core')
        core_cost  = operation.cpu.compute_nano_dollars(core_sku)

        ram_sku  = self.identify_google_compute_sku(operation, 'Ram')
        mem_cost  = operation.ram.compute_nano_dollars(ram_sku)

        disk_cost = 0.0
        for disk in operation.disks:
            disk_sku = self.identify_google_disk_sku(disk)
            disk_cost += disk.compute_nano_dollars(disk_sku)
        disk_usage = operation.get_disk_resource()

        return core_cost + mem_cost + disk_cost

    def identify_google_disk_sku(self, disk):
        formal_disk_names = self.google_disk_classes()

        common_name = disk.disk_label()
        sku_name = formal_disk_names[common_name]
        sku = self.sku_list[sku_name]
        return sku

    def identify_google_compute_sku(self, operation, resource_type):
        compute_class = self.identify_google_compute_class(operation)
        formal_region = self.identify_google_compute_formal_region(operation)

        if 'with' in compute_class:
            # special. Only one sku.
            return self.sku_list[name]

        proper_sku_name = None
        if operation.preemptible:
            template = 'Preemptible {unit} {resource_type} running in {region}'
            proper_sku_name = template.format(
                unit=compute_class,
                resource_type=resource_type,
                region=formal_region
            )
        else:
            proper_sku_name = '{} {}'.format(compute_class, resource_type)

        return self.sku_list[proper_sku_name]

    def identify_google_compute_formal_region(self, operation):
        formal_region_names = self.google_alternative_region_names()

        region, _ = operation.zone.rsplit('-', 1)

        formal_region = None
        if region in self.formal_region_name:
            formal_region = formal_region_names[region]
        else:
            super_region, _ = region.split('-')
            formal_region = formal_region_names[super_region]

        return formal_region

    def identify_google_compute_class(self, operation):
        _, cpus, mem_mb = operation.machine.split('-')
        compute_key = (cpus, mem_mb)
        available_machines = self.get_available_compute_types(
            operation.zone,
            operation.project
        )
        compute_classes = self.google_compute_classes()
        name = None
        if compute_key in available_machines:
            name = available_machines[compute_key]['name']
        else:
            name = 'custom'

        compute_class = compute_classes[name]
        return compute_class

    def google_compute_classes(self):
        resource_classes = {
            'n1-highcpu': 'N1 High-CPU Instance',
            'n1-highmem': 'N1 High-mem Instance',
            'n1-megamem': 'Memory Optimized',
            'n1-standard': 'N1 Standard Instance',
            'n1-ultramem': 'Memory Optimized',
            'f1-micro': 'Micro instance with burstable CPU',
            'g1-small': 'Small instance with 1 VCPU',
            'custom': 'Custom instance'
        }

    def google_alternative_region_names(self):
        region_formal_name = {
            'us-west2': 'Los Angeles',
            'us-east4': 'Virginia',
            'us': 'Americas'
        }

    def google_disk_classes(self):
        disk_classes = {
            'pd-ssd' : 'SSD backed PD Capacity',     # aka SSD provisioned space
            'pd-standard' : 'Storage PD Capacity',   # aka Standard provisioned space
        }

    def get_available_compute_types(self, zone, project):
        response = self.compute.machineTypes() \
                               .list(project=project, zone=zone)

        # assemble the relevant machines into a better format
        machines = {}
        while request is not None:
            response = request.execute()

            for machine_type in response['items']:
                key = (machine_type['guestCpus'], machine_type['memoryMb'])
                machines[key] = machine_type

            request = self.compute.machineTypes() \
                                  .list_next(previous_request=request,
                                             previous_response=response)

        return machines


# functions to gather the latest Google Cloud Platform Compute costs

#def machine_types():
#    compute_types = ('standard', 'highmem', 'highcpu')
#    compute_cores = (2, 4, 8, 16, 32, 64)
#
#    computes = {}
#    for t in compute_types:
#        for c in compute_cores:
#            formal_name = "{} Intel N1 {} VCPU running in Americas".format(t.capitalize(), c)
#            short_name = "n1-{}-{}".format(t, c)
#            computes[short_name] = formal_name
#
#    # add special cases
#    computes['n1-standard-1'] = "Standard Intel N1 1 VCPU running in Americas"
#    computes['f1-micro'] = "Micro instance with burstable CPU running in Americas"
#    computes['g1-small'] = "Small instance with 1 VCPU running in Americas"
#
#    return computes

#def disk_types():
#    # get the disks we usually care about (mostly standard provisioned and SSD compute in the USA)
#    # I manually crafted this list based on comparing the API list and cromwell metadata
#    disks = {
##        'CP-COMPUTEENGINE-LOCAL-SSD' : 'SSD backed Local Storage',
##        'CP-COMPUTEENGINE-STORAGE-PD-SNAPSHOT' : 'Storage PD Snapshot',   # aka "Snapshot storage"
#        'pd-ssd' : 'SSD backed PD Capacity',     # aka SSD provisioned space
#        'pd-standard' : 'Storage PD Capacity',   # aka Standard provisioned space
##        'CP-COMPUTEENGINE-LOCAL-SSD-PREMPTIBLE' : 'SSD backed Local Storage attached to Preemptible VMs'
#    }
#    # New Name: Storage Image -- not sure what old name this maps back onto????
#    return disks


#class GenomicsOperation(object):
#
#    def __init__(self, response_json):
#        meta = response_json['metadata']
#        vm =  meta['pipeline']['resources']['virtualMachine']
#        self.machine = vm['machineType'] # This is now likely to be something like custom-8-7424, even for pre-defined types.
#        self.zone = meta['events'][-1]['details']['zone'] # TODO - is this ok to always take the earliest event to get zone? Is it always VM starting?
#        self.region, _ = self.zone.rsplit('-', 1)
#        self.preemptible = vm['preemptible']
#        self.project = meta['pipeline']['resources']['projectId']
#        self.start_time = dateutil.parser.parse(
#            meta['startTime']
#            )
#
#        try:
#            self.end_time = dateutil.parser.parse(meta['endTime'])
#            self.length = self.end_time - self.start_time
#        except KeyError:
#            self.end_time = None
#            self.length = None
#
#        self.disks = [
#            Disk(x['sizeGb'], x['type']) for x in vm['disks']
#            ]
#        self.disks.append(Disk(vm['bootDiskSizeGb']))
#
#    def duration(self):
#        if self.length:
#            return self.length.total_seconds()
#        else:
#            return None
#
#    def __str__(self):
#        return ("(machine: {}, "
#                "zone: {}, "
#                "region: {}, "
#                "preemptible: {}, "
#                "start_time: {}, "
#                "end_time: {}, "
#                "length: {}, "
#                "duration: {} )").format(
#                        self.machine,
#                        self.zone,
#                        self.region,
#                        self.preemptible,
#                        self.start_time,
#                        self.end_time,
#                        self.length,
#                        self.duration()
#                        )

#class OperationCostCalculator(object):
#
#    def __init__(self, skus):
#        self.machine_types = MachineTypes()
#        self.disk_types = DiskTypes()
#        self.compute_skus = skus
#
#    def cost(self, operation):
#        return sum([self.resource_cost(x) for x in as_resources(operation)])
#
#    def price(self, resource):
#        if resource.type == 'disk':
#            sku = self.compute_skus[self.disk_types.formal_name(resource.name)]
#            price = get_disk_price(sku)
#        elif resource.type == 'compute':
#            import pdb; pdb.set_trace()
#            _, cpus, mem = resource.name.split('-')
#            names = self.machine_types.formal_names(
#                    resource.project,
#                    resource.zone,
#                    cpus,
#                    mem,
#                    resource.preemptible
#                    )
#            price = 0.0
#            for name, unit in zip(names, (cpus, int(mem) / 1024.0)):
#               component_price = get_compute_price(self.compute_skus[name]) * int(unit)
#               price += component_price
#        else:
#            msg = "Do not know how to handle resource type: '{}'".format(resource.type)
#            raise Exception(msg)
#        return price
#
#    def resource_cost(self, resource):
#        return resource.duration * self.price(resource) * resource.units
#

#def vm_resource_name(name, premptible):
#    identifier = 'CP-COMPUTEENGINE-VMIMAGE-{0}'.format(name.upper())
#    if premptible:
#        identifier = identifier + '-PREEMPTIBLE'
#    return identifier
#
#
#def disk_resource_name(type_):
#    return type_
#
#
#def vm_duration(duration):
#    '''Return the duration in hours with a minimum of 1 minutes.'''
#    # All machine types are charged a minimum of 10 minutes.
#    # See https://cloud.google.com/compute/pricing#billingmodel
#    if duration < 60:  # Enforce minimum of 1 minutes
#        price_duration = 60
#    return duration / 60.0 / 60.0
#
#def disk_lifetime(duration):
#    '''Return the duration in months based on a 30.5 day month.'''
#    # convert to months. Assuming a 30.5 day month or 732 hours based on
#    # footnote here: https://cloud.google.com/compute/pricing#localssdpricing
#    # rounding up the seconds based on https://cloud.google.com/compute/pricing#disk.
#    # Not sure if necessary depending on how timing is done in operations
#    # NOTE A 730 hour month may be more appropriate based on the above footnote
#    # and https://cloud.google.com/compute/pricing#billingmodel
#    return math.ceil(duration) / 60 / 60 / 24 / 30.5
#
#
#def vm_resource(op):
#    return Resource(
#            duration=vm_duration(op.duration()),
#            region=op.region,
#            name=op.machine,
#            units=1,
#            type='compute',
#            preemptible=op.preemptible,
#            zone=op.zone,
#            project=op.project
#            )
#
#
#def disk_resources(op):
#    return [Resource(
#                duration=disk_lifetime(op.duration()),
#                region=op.region,
#                name=disk_resource_name(d.type_),
#                units=d.size,
#                type='disk',
#                preemptible=op.preemptible,
#                zone=op.zone,
#                project=op.project
#                ) for d in op.disks
#            ]
#
#
#def as_resources(op):
#    resources = disk_resources(op)
#    resources.append(vm_resource(op))
#    return resources
#
#
#class DiskTypes(object):
#    def __init__(self):
#        self.disk_types = {
#            'pd-ssd' : 'SSD backed PD Capacity',     # aka SSD provisioned space
#            'pd-standard' : 'Storage PD Capacity',   # aka Standard provisioned space
#        }
#
#    def formal_name(self, name):
#        return self.disk_types[name]

#class MachineTypes(object):
#    def __init__(self):
#        credentials, project = google.auth.default()
#        self.service = discovery.build('compute', 'v1', credentials=credentials)
#        self.predefined_machines = {}
#        self._resource_classes = {
#            'n1-highcpu': 'N1 High-CPU Instance',
#            'n1-highmem': 'N1 High-mem Instance',
#            'n1-megamem': 'Memory Optimized',
#            'n1-standard': 'N1 Standard Instance',
#            'n1-ultramem': 'Memory Optimized',
#            'f1-micro': 'Micro instance with burstable CPU',
#            'g1-small': 'Small instance with 1 VCPU',
#            'custom': 'Custom instance'
#        }
#        self._region_formal_name = {
#            'us-west2': 'Los Angeles',
#            'us-east4': 'Virginia',
#            'us': 'Americas'
#        }
#
#
#    def _retrieve_from_api(self, project, zone):
#        machines = self.predefined_machines.setdefault(project, {}).setdefault(zone, {})
#        request = self.service.machineTypes().list(project=project, zone=zone)
#        while request is not None:
#            response = request.execute()
#            for machine_type in response['items']:
#                key = (machine_type['guestCpus'], machine_type['memoryMb'])
#                machines[key] = machine_type
#            request = self.service.machineTypes().list_next(previous_request=request, previous_response=response)
#
#    def resource_group(self, project, zone, cpus, memoryMb):
#        if project not in self.predefined_machines or zone not in self.predefined_machines[project]:
#            self._retrieve_from_api(project, zone)
#        key = (cpus, memoryMb)
#        try:
#            name = self.predefined_machines[project][zone][key]['name']
#        except KeyError:
#            name = 'custom'
#        return self._resource_classes[name]
#
#    def region_string(self, zone):
#        region, _ = zone.rsplit('-', 1)
#        if region in self._region_formal_name:
#            return self._region_formal_name[region]
#        else:
#            super_region, _ = region.split('-')
#            return self._region_formal_name[super_region]
#
#    def formal_names(self, project, zone, cpus, memoryMb, preemptible):
#        # return two formal names, one for CPU, one for RAM
#        # note that for g1-small and f1-micro, there is but one
#        name = self.resource_group(project, zone, cpus, memoryMb)
#        region_string = self.region_string(zone)
#        if 'with' in name:
#            # special. Only one sku.
#            units = (name)
#        else:
#            units = [' '.join((name, x)) for x in ['Core', 'Ram']]
#            if preemptible:
#                units = ['Preemptible ' + x for x in units]
#                return [ '{unit} running in {region}'.format(unit=x, region=self.region_string(zone)) for x in units]
#
#Resource = namedtuple('Resource', ['duration', 'region', 'name', 'units', 'type', 'preemptible', 'zone', 'project'])
