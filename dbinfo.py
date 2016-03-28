#!/usr/bin/env python

# Script for obtaining various information datapoints about a database inside the Cloudant DBaaS
# Utilizes the standard Cloudant API found at http://docs.cloudant.com
# Written by Brad Bonn
# github@theotter.net
# IBM Cloud Data Services

# Required environment variables:
# CLOUDANT_AUTH="Basic: <base64auth>" (Tries to use this first)
# CLOUDANT_ADMIN_AUTH="Basic: <base64auth>" (If this exists, overrides and uses it instead)

 #Always Mandatory Parameters:  (Providing only these will give a summary of the DB stats)
 #-u <Cloudant username>
 #-d <Cloudant database>
 #
 #Always Optional parameters:
 #-v (Be verbose)
 #
 #Additional Mode Parameters: (Add these to obtain more detailed information about the database.)
 # For a map of shard distributions on cluster nodes: *requires dedicated cluster admin privileges
 #   -s [-k] (adds a legend of shard distribution ranges)
 # For a list of indexes (views and search) and their associated stats:
 #   -i
 # For total # of conflicts in the database:
 #   -x

import time, argparse, string, os, sys, re, json, requests

config = dict(
    cluster = '',
    account = '',
    dbname = '',
    db_size = 0,
    my_header = dict(),
    shards = dict(),
    doc_count = 0,
    batch = 10000, # Number of docs returned in each GET query for the conflict scan
    time_estimate_ratio = (4 * 60.0) / 144250.0, # estimated number of seconds per doc based on 4kb doc size
    too_long = 120, # Number of seconds that triggers the script to confirm if user really wants conflict scan
    fatal_too_long = 3600, # number of seconds that we offer an alternative to perform a conflict scan
    too_big = 50 * 1024 * 1024, # data size version of too_long (bytes)
    fatal_too_big = 1024 * 1024 * 1024, # data size version of fatal_too_long (bytes)
    verbose = False
    )

# Main
def main(argv):
    
    argparser = argparse.ArgumentParser(description = 'Get helpful information about a database inside Cloudant DBaaS')
    argparser.add_argument(
        'account',
        type=str,
        help='Cloudant account name (https://<account>.cloudant.com)'
    )
    argparser.add_argument(
        'database',
        type=str,
        help='Cloudant database name'
    )
    argparser.add_argument(
        '-s',
        action='store_true',
        help='Display shard information for database'
    )
    argparser.add_argument(
        '-x',
        action='store_true',
        help = 'Check DB for conflicts (slow)'
    )
    argparser.add_argument(
        '-i',
        action='store_true',
        help = 'Display index information'
    )
    argparser.add_argument(
        '-v',
        action='store_true',
        help = 'Be verbose'
    )
    myargs = argparser.parse_args()
    config['account'] = myargs.account
    config['dbname'] = myargs.database
    print_shards = myargs.s
    print_conflicts = myargs.x
    print_indexes = myargs.i
    config['verbose'] = myargs.v
    
    # Set authentication up        
    adminauthstring = os.environ.get('CLOUDANT_ADMIN_AUTH')
    authstring = os.environ.get('CLOUDANT_AUTH')
    if len(adminauthstring) > 0:
        authstring = adminauthstring
    elif len(authstring) == 0:
        sys.exit("ERROR: Required environment variables not set")
    
    config['my_header'] = {'Content-Type': 'application/json', 'Authorization': authstring}
    config['shards'] = get_shards() # Get the JSON of shard data {"shard1": [node,node,node], "shard2"...} AND get cluster name!
    
    # Print summary data
    get_summary()

    # Get and print index info
    if (print_indexes):
        get_indexes()
        
    # Get and print conflict count
    if (print_conflicts):
        get_conflicts()
    
    # Get and print shard details
    if (print_shards):
        nodes = get_node_list()
        print " Distribution of shards for database "+ config['dbname'] +" on cluster: " + config['cluster']
        print_shard_map(nodes, config['shards'])

def get_conflicts():
    est_time = config['doc_count'] * config['time_estimate_ratio']
    if (est_time > config['fatal_too_long'] or config['db_size'] > config['fatal_too_big']):
        print " A conflict scan for \"{0}\" would need an estimated {1} of bandwidth and {2} to complete".format(config['dbname'],data_size_pretty(config['db_size']), pretty_time(est_time))
        print " Create a view to perform this operation instead. \n Follow the instructions found at:"
        print " https://docs.cloudant.com/mvcc.html#distributed-databases-and-conflicts"
        return
    elif (est_time > config['too_long']) or (config['db_size'] > config['too_big']):
        print " !!!ATTENTION!!! Conflict scan will need {0} of bandwidth and take about {1} !!!ATTENTION!!!".format(data_size_pretty(config['db_size']), pretty_time(est_time))
        yes = raw_input(" Are you sure? (Y/N): ")
        if yes not in ("y","Y"):
            return
    print "     Scanning for conflicts. Progress:"
    rows = 0
    progress = 0
    conflict_count = 0
    bar_width = 40
    bar_increment = config['doc_count'] / bar_width
    sys.stdout.write("[%s]" % (" " * bar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (bar_width + 1 ))
    starttime = time.time()
    
    def increment():
        if (int(progress) / int(bar_increment)) == (int(progress) / float(bar_increment)):
            return True
        else:
            return False

    while (progress < config['doc_count']):
        myurl = 'https://{0}.cloudant.com/{1}/_all_docs?include_docs=true&conflicts=true&limit={2}&skip={3}'.format(config['account'],config['dbname'],config['batch'],rows)
        r = requests.get(
            myurl,
            headers = config['my_header']
        )
        if r.status_code not in (200,201,202):
            sys.exit("Failed, bad HTTP response")
        
        # temp
        #print myurl
        #print "rows: {0}, progress: {1}, docs: {2}, returned this call: {3}".format(rows,progress, config['doc_count'], len(r.json()['rows']))
        
        # find any incidents of conflicts in rows:
        if len(r.json()['rows']) > 0:
            for row in r.json()['rows']:
                try:
                    conflicts = len(row['doc']['_conflicts'])
                    conflict_count = conflict_count + conflicts
                except:
                    pass
                progress = progress + 1
                if (increment()):
                    sys.stdout.write("-")
                    sys.stdout.flush()
            rows = rows + len(r.json()['rows'])
        
        
    sys.stdout.write("\n")
    endtime = time.time()
    totaltime = endtime - starttime
    print " {0} conflicts found in {1}".format(conflict_count, pretty_time(totaltime))
    print ""

def get_indexes():
    pass
    myurl = 'https://{0}.cloudant.com/{1}/_all_docs?startkey="_design/"&endkey="_design0"'.format(config['account'],config['dbname'])
    r = requests.get(
        myurl,
        headers = config['my_header']
    )
    if r.status_code not in (200,201,202):
        sys.exit("Failed, bad HTTP response")
    json_response = r.json()
    
    total_ddoc_sizes = dict(
        Views = 0,
        Geo = 0,
        Search = 0
    )
    
    # These can be consolidated better
    viewline = (" "*4)+'{0:8}:  "{1}"'
    geoline = (" "*2)+'{0:5}: "{1}"  {2}'
    searchline = (" "*2)+'{0:5}: "{1}"  {2}'
    
    print "Design documents:"
    for row in json_response['rows']:
        ddocurl = 'https://{0}.cloudant.com/{1}/{2}'.format(config['account'],config['dbname'],row['key'])
        r2 = requests.get(
            ddocurl,
            headers = config['my_header']
        )
        if r2.status_code not in (200,201,202):
            sys.exit("Failed, bad HTTP response")
        ddoc_content = r2.json()
        #ddoc_name = ddoc_content['_id']
        ddoc_name = re.sub('_design/', '', ddoc_content['_id'])
        ddoc_buffer = ' ' + ('-'*(50-len(ddoc_name)))
        print ' "' + ddoc_name + '" ' + ddoc_buffer
        
        for key,value in ddoc_content.items():
            if (key == "views" and len(value) > 0):
                total_ddoc_sizes['Views'] = total_ddoc_sizes['Views'] + print_view_size(ddoc_name)
                if (config['verbose']):
                    for viewname,viewdata in value.items():
                        if ('reduce' in viewdata.keys() and 'options' in viewdata.keys()):
                            viewtype = 'Query'
                        elif ('reduce' in viewdata.keys()):
                            viewtype = 'Map/R'
                        else:
                            viewtype = 'Map'
                        print viewline.format(viewtype,viewname)
            elif key == "indexes":
                for indexname in value.keys():
                    search_size = get_search_size(ddoc_name,indexname)
                    total_ddoc_sizes['Search'] = total_ddoc_sizes['Search'] + search_size
                    print searchline.format('Search Index',indexname, data_size_pretty(search_size))
            elif key == "st_indexes":
                for geo in value.keys():
                    geo_size = get_geo_size(ddoc_name,geo)
                    total_ddoc_sizes['Geo'] = total_ddoc_sizes['Geo'] + geo_size
                    print geoline.format('Geo Index',geo,data_size_pretty(geo_size))
    print ""
    print " Total index sizes across database:"
    for key,value in total_ddoc_sizes.items():
        print '{0:>7}: {1:>10}'.format(key,data_size_pretty(value))
    print ""

def get_search_size(ddoc,index):
    myurl = 'https://{0}.cloudant.com/{1}/_design/{2}/_search_info/{3}'.format(
        config['account'],
        config['dbname'],
        ddoc,
        index
    )
    r = requests.get(
        myurl,
        headers = config['my_header']
    )
    if r.status_code not in (200,201,202):
        sys.exit("Failed, bad HTTP response")
    search_info = r.json()
    search_size = search_info['search_index']['disk_size']
    return search_size

def get_geo_size(ddoc,index):
    myurl = 'https://{0}.cloudant.com/{1}/_design/{2}/_geo_info/{3}'.format(
            config['account'],
            config['dbname'],
            ddoc,
            index
        )
    r = requests.get(
        myurl,
        headers = config['my_header']
    )
    if r.status_code not in (200,201,202):
        sys.exit("Failed, bad HTTP response")
    geo_info = r.json()
    geo_size = geo_info['geo_index']['disk_size']
    return geo_size
    
def print_view_size(ddoc):
    myurl = 'https://{0}.cloudant.com/{1}/_design/{2}/_info'.format(
            config['account'],
            config['dbname'],
            ddoc
        )
    r = requests.get(
        myurl,
        headers = config['my_header']
    )
    if r.status_code not in (200,201,202):
        sys.exit("Failed, bad HTTP response")
    view_info = r.json()
    view_size = view_info['view_index']['sizes']['file']
    if view_size > 0:
        print '  Views: {0}'.format(data_size_pretty(view_size))
    return view_size
    
def get_summary():
    # Summary stats include:
    # json data size (from sizes: active)
    # json disk size (includes un-compacted data, from sizes: file)
    # doc count (from doc_count)
    # deleted doc count (from doc_del_count)
    myurl = 'https://{0}.cloudant.com/{1}'.format(config['account'],config['dbname'])
    r = requests.get(
        myurl,
        headers = config['my_header']
    )
    if r.status_code not in (200,201,202):
        sys.exit("Failed, bad HTTP response")
    stats = r.json()
    if (stats['doc_count'] == 0 and not print_shards):
        sys.exit(" Database exists, but is empty. Exiting")
    config['doc_count'] = stats['doc_count']
    print ""
    print " Summary Info for Cloudant Database: \"{0}\"  In Account: \"{1}\"".format(config['dbname'],config['account'])
    shardcount = len(config['shards'])
    nvalue = len(config['shards'].itervalues().next())
    print " Unique shards (Q): {0}  Replica setting (N): {1}".format(shardcount,nvalue)
    counts = [
        count_pretty(stats['doc_count']),
        count_pretty(stats['doc_del_count'])
    ]
    print " JSON Document Count: {0} with {1} deleted doc 'tombstones'".format(counts[0],counts[1])
    
    #temp 
    #print stats
    
    datasizes = []
    # Account for small or empty databases, where the API gets weird on disk space
    if stats['sizes']['active'] == None:
        datasizes.append(data_size_pretty(stats['sizes']['external']))
    else:
        datasizes.append(data_size_pretty(stats['sizes']['active']))
    
    datasizes.append(data_size_pretty(stats['sizes']['file']))
    
    print " JSON Data size: {0} operating, {1} on disk".format(datasizes[0],datasizes[1])
    
    del_doc_est_size = stats['doc_del_count'] * 200 # 127 bytes of JSON and some allowances for disk overhead
    
    print " Estimated space overhead from tombstones: {0}".format(data_size_pretty(del_doc_est_size))
    
    percent_overhead = (float(stats['doc_del_count']) / float(stats['doc_count'])) * 100
    
    print " Estimated primary index overhead from tombstones: {0} %".format(round(percent_overhead,2))
    config['db_size'] = stats['sizes']['active']
    print ""

def count_pretty(size):
    return "{:,}".format(size)
    
def data_size_pretty(size):
    measure = 0
    size = float(size)
    while (size > 1024):
        size = round(size / 1024, 2)
        measure = measure + 1
    codes = [' b ',' KB',' MB',' GB',' TB',' PB']
    if measure == 0:
        size = int(size)
    formattedsize = "{:,}".format(size)
    return (formattedsize + codes[measure])    
    
def get_shards():
    myurl = 'https://{0}.cloudant.com/{1}/_shards'.format(config['account'],config['dbname'])
    r = requests.get(
        myurl,
        headers = config['my_header']
    )
    if r.status_code not in (200,201,202):
        sys.exit("Failed, bad HTTP response")
    json_response = r.json()
    nodelist = json_response['shards'].itervalues().next()
    m = re.search('.*\.(.+?)\.cloudant\.net', nodelist[0])
    config['cluster'] = m.group(1)
    return json_response['shards']

def get_node_list():
    nodes = []
    myurl = 'https://' + config['cluster'] + '.cloudant.com/_membership'
    r = requests.get(
        myurl,
        headers = config['my_header']
    )
    json_response = r.json()
    for nodestring in json_response['cluster_nodes']:
        nodes.append(strip_nodename(nodestring))
    return(nodes)

def strip_nodename(fullname):
    garbage1 = 'dbcore@db'
    garbage2 = '.' + config['cluster'] + '.cloudant.net'
    # regex out the node name
    without_tail = re.sub(garbage2,'',fullname)
    nodename = re.sub(garbage1,'',without_tail)
    return (int(nodename))

def print_shard_map(nodes, shards):
    distribution = dict()
    lines_to_print = []
    width = 6
    # Make an empty list for each node so the node will still print 
    for node in nodes:
        distribution[node] = []
   
    # choose number of characters to show based on shard count
    if len(shards) < 16:
        trim = 1
    elif len(shards) < 256:
        trim = 2
    else:
        trim = 3
        
    # Populate distribution of shards on each 
    for shardrange,nodes in shards.iteritems():
        for longnode in nodes:
            node = strip_nodename(longnode)
            try:
                distribution[node].append(shardrange[:trim])
            except:
                sys.exit(" ERROR: Node status unavailable: db{0}".format(node))
    for node in sorted(distribution):
        shardlist = ','.join(sorted(distribution[node]))
        lines_to_print.append([node, shardlist, len(distribution[node])])
        if width < len(shardlist):
            width = len(shardlist)
    
    # Set format string up based on max shard column width
    formatstring = "|{0:>5} |{1:^6}|{2:<"+ str(width) + "}|"
    headerstring = re.sub('<','^',formatstring)
    # print
    print "_" * (width + 16)
    print headerstring.format('Node','Shards','Ranges')
    print "-" * (width + 16)
    for line in lines_to_print:
        print formatstring.format(line[0],line[2],line[1])
    print "-" * (width + 16)
       
def pretty_time(seconds):
    seconds = float(seconds)
    if seconds >= 3600:
        time = round(seconds / 3600 , 1)
        measure = ' hours'
    elif seconds >= 60:
        time = round(seconds / 60, 1)
        measure = ' minutes'
    else:
        time = round(seconds, 2)
        measure = ' seconds'
    return (str(time)+measure)

if __name__ == "__main__":
    main(sys.argv[1:])

