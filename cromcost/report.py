from __future__ import division, print_function
from pprint import pprint

def standard_cost_report(json_costs):
    print(json.dumps(json_costs, sort_keys=True, indent=4))
    total_cost = 0.0
    for k in json_costs.keys():
        task_cost = json_costs[k]['total-cost']
        total_cost += task_cost
        print("{} : {}".format(k, task_cost))
    print("Total Cost: {}".format(total_cost))
