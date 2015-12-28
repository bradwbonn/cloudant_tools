#!/usr/bin/env python
# Fast and dirty script to grab -u username -c cluster -d database shard distributions.
# Add -k to add a legend of which shard range corresponds with each letter code
# Set your environment variable "CLOUDANT_ADMIN_AUTH" to an auth string (i.e. "Basic: <base64auth>")
# Right now, assumes you've not fat-fingered anything

import requests
import json
import re
import sys
import getopt
import os
import string

authstring = os.environ.get('CLOUDANT_ADMIN_AUTH')
my_header = {'Content-Type': 'application/json', 'Authorization': authstring}
account = ''
config = dict(
    cluster = '',
    account = '',
    dbname = ''
    )
shards = dict()

# Main
def main(argv):
    print_legend = False
    try:
        opts, args = getopt.getopt(argv,"u:c:d:k")
    except getopt.GetoptError:
        print "type better."
        sys.exit(2)
        
    for opt, arg in opts:
        if opt == '-u':
            config['account'] = arg
        elif opt in ("-d"):
            config['dbname'] = arg
        elif opt in ("-c"):
            config['cluster'] = arg
        elif opt in ("-k"):
            print_legend = True
            
    nodes = get_node_list()
    
    shardlist = get_shards() # Get the JSON of shard data {"shard1": [node,node,node], "shard2"...}
    
    shardtable = make_shard_table(shardlist)
    
    print_header(len(shardlist), shardtable['nvalue'])

    if (print_legend): # print shard legend if user-requested
        print_shard_key(shardtable) 
    else:
        print_shard_map(nodes, shardlist, shardtable)
    
def get_shards():
    myurl = 'https://{0}.cloudant.com/{1}/_shards'.format(config['account'],config['dbname'])
    r = requests.get(
        myurl,
        headers = my_header
    )
    if r.status_code not in (200,201,202):
        sys.exit("Failed, bad HTTP response")
    json_response = r.json()
    return json_response['shards']

def get_node_list():
    nodes = []
    myurl = 'https://' + config['cluster'] + '.cloudant.com/_membership'
    r = requests.get(
        myurl,
        headers = my_header
    )
    json_response = r.json()
    for nodestring in json_response['cluster_nodes']:
        nodes.append(strip_nodename(nodestring))
    return(nodes)

def strip_nodename(fullname):
    garbage1 = 'dbcore@'
    garbage2 = '.' + config['cluster'] + '.cloudant.net'
    # regex out the node name
    without_tail = re.sub(garbage2,'',fullname)
    nodename = re.sub(garbage1,'',without_tail)
    return (nodename)

def print_shard_map(nodes, shards, shardtable):
    distribution = dict()
    # Make an empty list for each node so the node will still print 
    for node in nodes:
        distribution[node] = []
    print " Shard distribution balance:"
    for shardrange,nodes in shards.iteritems():
        for longnode in nodes:
            node = strip_nodename(longnode)
            distribution[node].append(shardtable[shardrange])
    for node in sorted(distribution):
        if (len(distribution[node]) > 0):
            shardlist = ''.join(distribution[node]) 
            print " {0}: {1} ({2})".format(node,shardlist,len(shardlist))
        else:
            print " {0}: < None >".format(node)
    print ""

def print_shard_key(shards):
    print " Shard range:        Code:"
    #######e0000000-ffffffff
    for shardrange in sorted(shards):
        print " {0}: {1}".format(shardrange,shards[shardrange])
    print " ---"
    
def make_shard_table(shards):
    alphacodes = list(string.ascii_lowercase + string.ascii_uppercase + string.digits + string.punctuation)
    code = 0
    resulttable = dict()
    for shardrange in sorted(shards):
        resulttable[shardrange] = alphacodes[code]
        code = code + 1
        if code >= len(alphacodes):
            sys.exit("Oops, Q > {0}.".format(len(alphacodes)))
        elif code == 1:
            resulttable['nvalue'] = len(shards[shardrange])
    return (resulttable)
    
def print_header(shardcount, nvalue):
    print ""
    print " Distribution of shards for database "+ config['dbname'] +" on cluster: " + config['cluster']
    print " Unique shards (Q): {0}  Replica setting (N): {1}".format(shardcount,nvalue)
    

if __name__ == "__main__":
    main(sys.argv[1:])

