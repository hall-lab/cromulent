from __future__ import division

import dateutil.parser
import math, os, sys, json
import logging
from collections import namedtuple

from googleapiclient import discovery
import google.auth
import requests

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

    def is_multi_tiered_pricing(self, sku):
        tiered_rates = (sku['pricingInfo'][0]
                           ['pricingExpression']
                           ['tieredRates'])
        bool = True if len(tiered_rates) >= 2 else False
        return bool

    def get_unit_price(self, sku):
        # simple case -- single tiered rates
        return int(sku['pricingInfo'][0]
                      ['pricingExpression']
                      ['tieredRates'][0]
                      ['unitPrice']
                      ['nanos'])

    def get_base_unit_conversion_factor(self, sku):
        return float(sku['pricingInfo'][0]
                        ['pricingExpression']
                        ['baseUnitConversionFactor'])

class Disk(Resource):

    def __init__(self, size, duration, disk_type='pd-standard'):
        self.size = size # in gb
        self.type_ = disk_type
        super(Disk, self).__init__(duration=duration)

    def disk_label(self):
        return self.type_

    def get_unit_prices(self, sku):
        # multi-tiered rate case
        tiered_rates = (sku['pricingInfo'][0]
                           ['pricingExpression']
                           ['tieredRates'])

        return tiered_rates

    # All disk-related charges are prorated on seconds
    # Calculation based on reading:
    # https://cloud.google.com/billing/reference/rest/v1/services.skus/list#PricingExpression
    def compute_nano_dollars(self, sku, tier_scheme):
        cost_methodology = {
            'all'       : self.cost_all_tier,
            'no-free'   : self.cost_no_free_tier,
            'top-tier'  : self.cost_top_tier,
            'max-price' : self.cost_max_price_tier
        }

        fn = cost_methodology[tier_scheme]
        total_cost = fn(sku)
        return total_cost

    def _compute_unit_disk_usage(self):
        seconds = self.duration
        months = seconds / 60.0 / 60.0 / 24.0 / 30.0
        unit_disk_usage = self.size * months # in gb * month
        return unit_disk_usage

    def _single_tier_cost(self, tier, sku):
        unit_price = tier['unitPrice']['nanos']
        base_price = self.get_base_price(unit_price, sku) # nano dollars / (byte * second)

        bytes_ = self.size * 1024.0 * 1024.0 * 1024.0
        seconds = self.duration
        base_unit_usage = bytes_ * seconds
        nano_dollars = base_unit_usage * base_price
        return nano_dollars

    def _multi_tier_cost(self, initial_disk_usage, tiers, sku):
        unit_disk_usage = initial_disk_usage

        total_cost = 0.0
        for tier in tiers:
            tier_unit_disk_usage_amount = unit_disk_usage - tier['startUsageAmount'] # in gb * month
            base_disk_usage = self.get_base_units(tier_unit_disk_usage_amount, sku)
            unit_price = tier['unitPrice']['nanos']
            base_price = self.get_base_price(unit_price, sku) # nano dollars / (byte * second)
            nano_dollars = base_disk_usage * base_price
            total_cost += nano_dollars
            unit_disk_usage = tier['startUsageAmount']

        return total_cost

    def cost_max_price_tier(self, sku):
        tiered_unit_prices = self.get_unit_prices(sku)
        max_tier = max(tiered_unit_prices, key=lambda e: e['unitPrice']['nanos'])
        nano_dollars = self._single_tier_cost(max_tier, sku)
        return nano_dollars

    def cost_top_tier(self, sku):
        tiered_unit_prices = self.get_unit_prices(sku)
        top_tier = tiered_unit_prices[-1]
        nano_dollars = self._single_tier_cost(top_tier, sku)
        return nano_dollars

    def cost_no_free_tier(self, sku):
        tiered_unit_prices = self.get_unit_prices(sku)

        # if the first tier is "free" set the price of the free tier to the
        # price of the next higher tier
        if tiered_unit_prices[0]['unitPrice']['nanos'] == 0:
            tiered_unit_prices[0]['unitPrice']['nanos'] = \
                tiered_unit_prices[1]['unitPrice']['nanos']

        unit_disk_usage = self._compute_unit_disk_usage()

        relevant_tiers = [ x for x in tiered_unit_prices
                             if x['startUsageAmount'] < unit_disk_usage ]

        relevant_tiers.reverse()

        nano_dollars = self._multi_tier_cost(unit_disk_usage, relevant_tiers, sku)
        return nano_dollars

    def cost_all_tier(self, sku):
        tiered_unit_prices = self.get_unit_prices(sku)
        unit_disk_usage = self._compute_unit_disk_usage()

        relevant_tiers = [ x for x in tiered_unit_prices
                             if x['startUsageAmount'] < unit_disk_usage ]

        relevant_tiers.reverse()

        nano_dollars = self._multi_tier_cost(unit_disk_usage, relevant_tiers, sku)
        return nano_dollars

    def get_base_units(self, unit_usage, sku):
        # unit usage is in (GiB * month)
        # base usage is in (byte * second)
        base_units = unit_usage * self.get_base_unit_conversion_factor(sku)
        return base_units

    def get_base_price(self, unit_price, sku):
        # unit price is in nano dollars / (GiB * month)
        # base price is in nano dollars / (byte * second)
        base_price = unit_price / self.get_base_unit_conversion_factor(sku)
        return base_price


class Cpu(Resource):

    def __init__(self, cores, duration):
        self.cores = int(cores)
        super(Cpu, self).__init__(duration=duration)

    # All cpu charges are prorated on seconds with min being 60 secs
    def compute_nano_dollars(self, sku):
        if self.is_multi_tiered_pricing(sku):
            sys.exit("[err] Please implement multi-tiered pricing for cpus!")

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

    def __init__(self, size, duration):
        self.size = size # in gb
        super(Ram, self).__init__(duration=duration)

    # All memory charges are prorated on seconds with min being 60 secs
    def compute_nano_dollars(self, sku):
        if self.is_multi_tiered_pricing(sku):
            sys.exit("[err] Please implement multi-tiered pricing for memory!")

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
        mem_gb = float(mem_mb) / 1024.0
        self.cpu = Cpu(cores=cpus, duration=time_elapsed)
        self.ram = Ram(size=mem_gb, duration=time_elapsed)

        # setup the disk components
        self.disks = [ Disk(size=x['sizeGb'],
                            disk_type=x['type'],
                            duration=time_elapsed) for x in vm['disks'] ]

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

        credentials, project = google.auth.default()
        self.credentials = credentials
        self.project = project

        self.billing  = discovery.build('cloudbilling', 'v1', credentials=credentials)
        self.compute  = discovery.build('compute', 'v1', credentials=credentials)
        self.genomics = discovery.build('genomics', 'v2alpha1', credentials=credentials)

        self.sku_list = self._construct_compute_sku_list(sku_path)

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
                response = self.billing.services().skus().list(parent=service_name, pageToken=response['nextPageToken']).execute()
            else:
                break

        return service_skus

    def get_genomics_operation_metadata(self, name):
        request = self.genomics.projects().operations().get(name=name)
        response = request.execute()
        return response

    def estimate_genomics_operation_cost(self, operation, tier_scheme):
        # a genomics operation cost consists of 3 components:
        #    1.  cpu/core usage
        #    2.  memory usage
        #    3.  disk usage
        # we need to calculate and return the cost of each component
        # in nano dollars (i.e. 1e9 nano dollars == 1 dollar)
        #
        # tier_scheme can be on of the following:
        # 1.  all       -- assume starting workflow in a new project
        #                  and include all the relevant tiering pricing
        # 2.  no-free   -- use tiered-pricing, but remove any free-tiers
        # 3.  top-tier  -- only use the pricing on the last/top tier
        # 4.  max-price -- use only the tier with the highest price
        core_sku = self.identify_google_compute_sku(operation, 'Core')
        core_cost  = operation.cpu.compute_nano_dollars(core_sku)

        ram_sku  = self.identify_google_compute_sku(operation, 'Ram')
        mem_cost  = operation.ram.compute_nano_dollars(ram_sku)

        disk_cost = 0.0
        for disk in operation.disks:
            disk_sku = self.identify_google_disk_sku(disk)
            disk_cost += disk.compute_nano_dollars(disk_sku, tier_scheme)

        return { 'cpu': core_cost, 'mem': mem_cost, 'disk': disk_cost }

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

        if proper_sku_name not in self.sku_list:
            sys.exit("[err] Didn't find '{}' in google sku list!".format(proper_sku_name))

        return self.sku_list[proper_sku_name]

    def identify_google_compute_formal_region(self, operation):
        formal_region_names = self.google_alternative_region_names()

        region, _ = operation.zone.rsplit('-', 1)

        formal_region = None
        if region in formal_region_names:
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
        return resource_classes

    def google_alternative_region_names(self):
        region_formal_name = {
            'us-west2': 'Los Angeles',
            'us-east4': 'Virginia',
            'us': 'Americas'
        }
        return region_formal_name

    def google_disk_classes(self):
        disk_classes = {
            'pd-ssd' : 'SSD backed PD Capacity',     # aka SSD provisioned space
            'pd-standard' : 'Storage PD Capacity',   # aka Standard provisioned space
        }
        return disk_classes

    def get_available_compute_types(self, zone, project):
        request = self.compute.machineTypes() \
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
