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

authstring = os.environ.get('CLOUDANT_ADMIN_AUTH')
my_header = {'Content-Type': 'application/json', 'Authorization': authstring}
account = ''
cluster = ''
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
            account = arg
        elif opt in ("-c"):
            cluster = arg
    nodes = get_node_list(cluster)
    for node in nodes:
        get_disk_state_of_node(node, account, cluster)
    print_results(cluster)

def get_node_list(cluster):
    nodes = []
    myurl = 'https://' + cluster + '.cloudant.com/_membership'
    r = requests.get(
        myurl,
        headers = my_header
    )
    json_response = r.json()
    garbage1 = 'dbcore@db'
    garbage2 = '.' + cluster + '.cloudant.net'
    for nodestring in json_response['cluster_nodes']:
        # regex out the node name
        without_tail = re.sub(garbage2,'',nodestring)
        nodename = re.sub(garbage1,'',without_tail)
        # add it to the list
        nodes.append(int(nodename))
    return(nodes)
    
def get_disk_state_of_node(node, account, cluster):
    urlformat = 'https://{0}.cloudant.com/_api/v2/monitoring/node_disk_{1}_srv?cluster={2}&format=json&node=db{3}'
    myurl = urlformat.format(
        account,
        'use',
        cluster,
        node
    )
    r = requests.get(
        myurl,
        headers = my_header
    )
    if r.status_code not in (200,201,202):
        print r.status_code
        sys.exit("Cannot query node stats: " + node)
    disk_used = r.json()
    myurl = urlformat.format(
        account,
        'free',
        cluster,
        node
    )
    r = requests.get(
        myurl,
        headers = my_header
    )
    if r.status_code not in (200,201,202):
        print r.status_code
        sys.exit("Cannot query node stats: " + node)
    disk_free = r.json()
    #bytes_used_datapoints = disk_used['target_responses']['datapoints']
    #bytes_free_datapoints = disk_free['target_responses']['datapoints']
    
    curr_used_key = len(disk_used['target_responses'][0]['datapoints']) - 2
    curr_free_key = len(disk_free['target_responses'][0]['datapoints']) - 2

    current_bytes_free = float(disk_free['target_responses'][0]['datapoints'][curr_free_key][0])
    previous_bytes_free = float(disk_free['target_responses'][0]['datapoints'][0][0])
    current_bytes_used = float(disk_used['target_responses'][0]['datapoints'][curr_used_key][0])
    previous_bytes_used = float(disk_used['target_responses'][0]['datapoints'][0][0])
    timediff = int(disk_free['target_responses'][0]['datapoints'][curr_free_key][1] - disk_free['target_responses'][0]['datapoints'][0][1])
    results[node] = [current_bytes_free,current_bytes_used,previous_bytes_free,previous_bytes_used,timediff]

def print_results(cluster):
    print ""
    print "Disk usage on the "+ str(len(results)) +" nodes of cluster: " + cluster
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
        print 'db{0:<3}:{1:>9} ({2:4}%){3} Change:{4}{5:>9} ({6}{7:4}% in {8:1}min)'.format(
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
    else:
        total_plusornot = "-"
        total_percent_change = abs(total_percent_change)
        total_change = abs(total_change)
    print ""
    print 'TOTAL:{0:>9} ({1:4}%)  Change:{2}{3:>9} ({4}{5:4}% in {6:1}min)'.format(
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
