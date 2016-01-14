#!/usr/bin/env python
# Fast and dirty script to grab -u username -c cluster disk space per node and print it out
# Set your environment variable "CLOUDANT_ADMIN_AUTH" to an auth string (i.e. "Basic: <base64auth>")

import requests
import json
import re
import sys
import getopt
import numpy
import os
from multiprocessing import Pool
from pprint import pprint

authstring = os.environ.get('CLOUDANT_ADMIN_AUTH')
my_header = {'Content-Type': 'application/json', 'Authorization': authstring}
config = dict(
    account = '',
    cluster = ''
)
results = dict()

# Main
def main(argv):
    # Check options for validity, print help if user fat-fingered anything
    try:
        opts, args = getopt.getopt(argv,"u:c:")
    except getopt.GetoptError:
        print "type better."
        sys.exit(2)
        
    for opt, arg in opts:
        if opt == '-u':
            config['account'] = arg
        elif opt in ("-c"):
            config['cluster'] = arg
            
    nodes = get_node_list()

    p = Pool()
    # Changed to map_async and added a 15-second timeout value. 
    results_array = p.map_async(get_disk_state_of_node, nodes).get(15)
    
    for node_data in results_array:
        results[node_data[0]] = [
            node_data[1],
            node_data[2],
            node_data[3],
            node_data[4],
            node_data[5]
        ]
        
    print_results()

def get_node_list():
    nodes = []
    myurl = 'https://' + config['cluster'] + '.cloudant.com/_membership'
    r = requests.get(
        myurl,
        headers = my_header
    )
    if r.status_code not in (200,201,202):
        sys.exit("Cannot obtain cluster node list. Check cluster name.")
    json_response = r.json()
    garbage1 = 'dbcore@db'
    garbage2 = '.' + config['cluster'] + '.cloudant.net'
    for nodestring in json_response['cluster_nodes']:
        # regex out the node name
        without_tail = re.sub(garbage2,'',nodestring)
        nodename = re.sub(garbage1,'',without_tail)
        # add it to the list
        nodes.append(int(nodename))
    return(nodes)
    
def get_disk_state_of_node(node):

    urlformat = 'https://{0}.cloudant.com/_api/v2/monitoring/node_disk_{1}_srv?cluster={2}&format=json&node=db{3}'
    myurl = urlformat.format(
        config['account'],
        'use',
        config['cluster'],
        node
    )

    r = requests.get(
        myurl,
        headers = my_header
    )
    if r.status_code not in (200,201,202):
        print r.status_code
        sys.exit("Cannot query node stats for " + str(node))
    raw_json = r.json()

    disk_used = raw_json['target_responses'][0]['datapoints']
    myurl = urlformat.format(
        config['account'],
        'free',
        config['cluster'],
        node
    )

    r = requests.get(
        myurl,
        headers = my_header
    )
    if r.status_code not in (200,201,202):
        print r.status_code
        sys.exit("Cannot query node stats: " + node)
    raw_json = r.json()

    disk_free = raw_json['target_responses'][0]['datapoints']
    
    # Find earlist and latest valid data points in each set of results
    # This behaves strangely if the API returns no timestamps.
    current_free = get_last_valid(disk_free)
    current_used = get_last_valid(disk_used)
    previous_free = get_first_valid(disk_free)
    previous_used = get_first_valid(disk_used)

    if current_free == 0 or current_used == 0 or previous_free == 0 or previous_used == 0:
        sys.exit("No valid statistics returned by API for db"+str(node))

    node_data = [
        node,
        current_free[0],
        current_used[0],
        previous_free[0],
        previous_used[0],
        int(current_free[1] - previous_free[1])
    ]

    return node_data

def get_last_valid(api_response):
    valid = []
    last = len(api_response) - 1
    for i in range(last,-1,-1):
        if isinstance(api_response[i][0],float):
            valid = [api_response[i][0], api_response[i][1]]
            return valid
        else:
            pass

    # If below runs, we can't find any datapoints with timestamps.
    print "No datapoints found!"
    return 0

def get_first_valid(api_response):
    valid = []
    last = len(api_response) - 1
    for i in range(0,last):
        if (isinstance(api_response[i][0],float)):
            valid = [api_response[i][0], api_response[i][1]]
            return valid
        else:
            pass
        
    # If below runs, we can't find any datapoints with timestamps.
    print "No datapoints found!"
    return 0

def print_results():
    print ""
    print " Disk usage on the "+ str(len(results)) +" nodes of cluster: " + config['cluster']
    total_percent_change = 0
    total_disk_used = 0
    total_disk_free = 0
    total_disk_previous = 0
    timediffs = []
    for key in sorted(results):
        result = results[key]
        change = result[1] - result[3]
        percent_change = round(((result[1] - result[3]) / result[3]) * 100, 1)
        percent_full = round((result[1] / (result[0] + result[1])) * 100, 1)
        disk_used = result[1]
        total_disk_previous = total_disk_previous + result[3]
        total_disk_used = total_disk_used + disk_used
        total_disk_free = total_disk_free + result[0]
        timediff = (result[4] / 60)
        timediffs.append(timediff)
        if percent_full > 90:
            tag = '*'
        else:
            tag = ' '
        if (change > 0):
            plusornot = "+"
        elif (change == 0):
            plusornot = ' '
        else:
            plusornot = "-"
            percent_change = abs(percent_change)
            change = abs(change)
        print ' db{0:<3}:{1:>10} ({2:4}%){3} Change:{4}{5:>10} ({6}{7:4}% in {8:1}min)'.format(
            key,
            data_size_pretty(disk_used),
            percent_full,
            tag,
            plusornot,
            data_size_pretty(change),
            plusornot,
            percent_change,
            timediff
        )
    total_change = total_disk_used - total_disk_previous
    total_percent_change = round(((total_disk_used - total_disk_previous) / total_disk_previous) * 100, 1)
    total_timediff = int(round(numpy.mean(timediffs)))
    total_percent_full = round((total_disk_used / (total_disk_free + total_disk_used)) * 100, 1)
    if total_change > 0:
        total_plusornot = "+"
    elif total_change < 0:
        total_plusornot = "-"
        total_percent_change = abs(total_percent_change)
        total_change = abs(total_change)
    else:
        plusornot = ' '
    print ""
    print ' TOTAL:{0:>10} ({1:4}%)  Change:{2}{3:>10} ({4}{5:4}% in {6:1}min)'.format(
        data_size_pretty(total_disk_used),
        total_percent_full,
        total_plusornot,
        data_size_pretty(total_change),
        total_plusornot,
        total_percent_change,
        total_timediff
    )
    print ""

def data_size_pretty(size):
    measure = 0
    size = float(size)
    while (size > 1024):
        size = round(size / 1024, 2)
        measure = measure + 1
    codes = [' b',' K',' M',' G',' T',' P']
    if measure == 0:
        size = int(size)
    formattedsize = "{:,}".format(size)
    return (formattedsize + codes[measure])  

if __name__ == "__main__":
    main(sys.argv[1:])
