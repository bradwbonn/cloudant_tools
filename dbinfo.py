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

def getargs():
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
    return argparser.parse_args()

class DBInfo(object):
    
    def __init__(self, account, dbname, verbose):
        self.account = account
        self.dbname = dbname
        self.verbose = verbose
        
        # Set authentication up. Default to admin auth.
        adminauthstring = os.environ.get('CLOUDANT_ADMIN_AUTH')
        authstring = os.environ.get('CLOUDANT_AUTH')
        if len(adminauthstring) > 0:
            authstring = adminauthstring
        elif len(authstring) == 0:
            sys.exit("ERROR: Required environment variables not set")
        self.my_header = {'Content-Type': 'application/json', 'Authorization': authstring}
        
        # Cluster and shard data.
        self.cluster = '' # (Filled in by function below)
        self.shards = self.get_shards()
        
        # Statistics to fill in
        self.doc_count = 0
        self.datasizes = []
        self.del_doc_est_size = 0
        self.percent_overhead = 0
        self.db_size = 0
        
        # Constants
        self.time_estimate_ratio = (4 * 60.0) / 144250.0 # estimated number of seconds per doc based on 4kb doc size
        self.batch = 10000 # Number of docs returned in each GET query for the conflict scan
        self.too_long = 120 # Number of seconds that triggers the script to confirm if user really wants conflict scan
        self.fatal_too_long = 3600 # number of seconds that we offer an alternative to perform a conflict scan
        self.too_big = 50 * 1024 * 1024 # data size version of too_long (bytes)
        self.fatal_too_big = 1024 * 1024 * 1024 # data size version of fatal_too_long (bytes)
        
    def json_get(self, url):
        r = requests.get(
            url,
            headers = self.my_header
        )
        if r.status_code not in (200,201,202):
            sys.exit("Failed, bad HTTP response")
        return r.json()
    
    def get_summary(self):
        # Summary stats include:
        # json data size (from sizes: active)
        # json disk size (includes un-compacted data, from sizes: file)
        # doc count (from doc_count)
        # deleted doc count (from doc_del_count)
        myurl = 'https://{0}.cloudant.com/{1}'.format(self.account,self.dbname)
        stats = self.json_get(myurl)
        
        if (stats['doc_count'] == 0 and not print_shards):
            sys.exit(" Database exists, but is empty. Exiting")
        self.doc_count = stats['doc_count']
        print ""
        print " Summary Info for Cloudant Database: \"{0}\"  In Account: \"{1}\"".format(self.dbname,self.account)
        shardcount = len(self.shards)
        nvalue = len(self.shards.itervalues().next())
        print " Unique shards (Q): {0}  Replica setting (N): {1}".format(shardcount,nvalue)
        counts = [
            self.count_pretty(stats['doc_count']),
            self.count_pretty(stats['doc_del_count'])
        ]
        print " JSON Document Count: {0} with {1} deleted doc 'tombstones'".format(counts[0],counts[1])
        
        #print stats
        
        # Account for small or empty databases, where the API gets weird on disk space
        if stats['sizes']['active'] == None:
            self.datasizes.append(self.data_size_pretty(stats['sizes']['external']))
        else:
            self.datasizes.append(self.data_size_pretty(stats['sizes']['active']))
        
        self.datasizes.append(self.data_size_pretty(stats['sizes']['file']))
        
        print " JSON Data size: {0} operating, {1} on disk".format(self.datasizes[0],self.datasizes[1])
        
        self.del_doc_est_size = stats['doc_del_count'] * 200 # 127 bytes of JSON and some allowances for disk overhead
        
        print " Estimated space overhead from tombstones: {0}".format(self.data_size_pretty(self.del_doc_est_size))
        
        self.percent_overhead = (float(stats['doc_del_count']) / float(stats['doc_count'])) * 100
        
        print " Estimated primary index overhead from tombstones: {0} %".format(round(self.percent_overhead,2))
        self.db_size = stats['sizes']['active']
        print ""
    
    def get_conflicts(self):
        est_time = self.doc_count * self.time_estimate_ratio
        if (est_time > self.fatal_too_long or self.db_size > self.fatal_too_big):
            print " A conflict scan for \"{0}\" would need an estimated {1} of bandwidth and {2} to complete".format(
                self.dbname,
                self.data_size_pretty(self.db_size),
                self.pretty_time(est_time)
                )
            print " Create a view to perform this operation instead. \n Follow the instructions found at:"
            print " https://docs.cloudant.com/mvcc.html#distributed-databases-and-conflicts"
            return
        elif (est_time > self.too_long) or (self.db_size > self.too_big):
            print " !!!ATTENTION!!! Conflict scan will need {0} of bandwidth and take about {1} !!!ATTENTION!!!".format(
                self.data_size_pretty(self.db_size),
                self.pretty_time(est_time)
                )
            yes = raw_input(" Are you sure? (Y/N): ")
            if yes not in ("y","Y"):
                return
        print "     Scanning for conflicts. Progress:"
        rows = 0
        progress = 0
        conflict_count = 0
        bar_width = 40
        bar_increment = self.doc_count / bar_width
        sys.stdout.write("[%s]" % (" " * bar_width))
        sys.stdout.flush()
        sys.stdout.write("\b" * (bar_width + 1 ))
        starttime = time.time()
        
        def increment():
            if (int(progress) / int(bar_increment)) == (int(progress) / float(bar_increment)):
                return True
            else:
                return False
    
        while (progress < self.doc_count):
            myurl = 'https://{0}.cloudant.com/{1}/_all_docs?include_docs=true&conflicts=true&limit={2}&skip={3}'.format(
                self.account,
                self.dbname,
                self.batch,
                rows
                )
            r = self.json_get(myurl)
            
            # find any incidents of conflicts in rows:
            if len(r['rows']) > 0:
                for row in r['rows']:
                    try:
                        conflicts = len(row['doc']['_conflicts'])
                        conflict_count = conflict_count + conflicts
                    except:
                        pass
                    progress = progress + 1
                    if (increment()):
                        sys.stdout.write("-")
                        sys.stdout.flush()
                rows = rows + len(r['rows'])
            
        sys.stdout.write("\n")
        endtime = time.time()
        totaltime = endtime - starttime
        print " {0} conflicts found in {1}".format(
            conflict_count,
            self.pretty_time(totaltime))
        print ""
    
    def count_pretty(self, size):
        return "{:,}".format(size)
    
    def data_size_pretty(self, size):
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
    
    def get_shards(self):
        myurl = 'https://{0}.cloudant.com/{1}/_shards'.format(self.account,self.dbname)
        json_response = self.json_get(myurl)
        nodelist = json_response['shards'].itervalues().next()
        m = re.search('.*\.(.+?)\.cloudant\.net', nodelist[0])
        self.cluster = m.group(1)
        return json_response['shards']
        
    def get_indexes(self):
        myurl = 'https://{0}.cloudant.com/{1}/_all_docs?startkey="_design/"&endkey="_design0"'.format(
            self.account,
            self.dbname
            )
        json_response = self.json_get(myurl)
        
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
            ddocurl = 'https://{0}.cloudant.com/{1}/{2}'.format(
                self.account,
                self.dbname,
                row['key']
                )
            ddoc_content = self.json_get(ddocurl)

            ddoc_name = re.sub('_design/', '', ddoc_content['_id'])
            ddoc_buffer = ' ' + ('-'*(50-len(ddoc_name)))
            print ' "' + ddoc_name + '" ' + ddoc_buffer
            
            for key,value in ddoc_content.items():
                if (key == "views" and len(value) > 0):
                    total_ddoc_sizes['Views'] = total_ddoc_sizes['Views'] + self.print_view_size(ddoc_name)
                    if (self.verbose):
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
                        search_size = self.get_search_size(ddoc_name,indexname)
                        total_ddoc_sizes['Search'] = total_ddoc_sizes['Search'] + search_size
                        print searchline.format('Search Index',indexname, self.data_size_pretty(search_size))
                elif key == "st_indexes":
                    for geo in value.keys():
                        geo_size = self.get_geo_size(ddoc_name,geo)
                        total_ddoc_sizes['Geo'] = total_ddoc_sizes['Geo'] + geo_size
                        print geoline.format('Geo Index',geo,self.data_size_pretty(geo_size))
        print ""
        print " Total index sizes across database:"
        for key,value in total_ddoc_sizes.items():
            print '{0:>7}: {1:>10}'.format(key,self.data_size_pretty(value))
        print ""

    def get_search_size(self, ddoc, index):
        myurl = 'https://{0}.cloudant.com/{1}/_design/{2}/_search_info/{3}'.format(
            self.account,
            self.dbname,
            ddoc,
            index
        )
        search_info = self.json_get(myurl)
        search_size = search_info['search_index']['disk_size']
        return search_size

    def get_geo_size(self, ddoc, index):
        myurl = 'https://{0}.cloudant.com/{1}/_design/{2}/_geo_info/{3}'.format(
                self,account,
                self.dbname,
                ddoc,
                index
            )
        geo_info = self.json_get(myurl)
        geo_size = geo_info['geo_index']['disk_size']
        return geo_size

    def print_view_size(self, ddoc):
        myurl = 'https://{0}.cloudant.com/{1}/_design/{2}/_info'.format(
                self.account,
                self.dbname,
                ddoc
            )
        view_info = self.json_get(myurl)
        view_size = view_info['view_index']['sizes']['file']
        if view_size > 0:
            print '  Views: {0}'.format(self.data_size_pretty(view_size))
        return view_size

    def get_node_list(self):
        nodes = []
        myurl = 'https://' + self.cluster + '.cloudant.com/_membership'
        json_response = self.json_get(myurl)
        for nodestring in json_response['cluster_nodes']:
            nodes.append(self.strip_nodename(nodestring))
        print " Distribution of shards for database "+ self.dbname +" on cluster: " + self.cluster
        self.print_shard_map(nodes, self.shards)

    def strip_nodename(self, fullname):
        garbage1 = 'dbcore@db'
        garbage2 = '.' + self.cluster + '.cloudant.net'
        # regex out the node name
        without_tail = re.sub(garbage2,'',fullname)
        nodename = re.sub(garbage1,'',without_tail)
        return (int(nodename))

    def pretty_time(self, seconds):
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

    def print_shard_map(self, nodes, shards):
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
                node = self.strip_nodename(longnode)
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

# Main
def main():

    myargs = getargs()
    
    dbinfo = DBInfo(myargs.account, myargs.database, myargs.v)
    
    # Print summary data
    dbinfo.get_summary()

    # Get and print index info
    if (myargs.i):
        dbinfo.get_indexes()
        
    # Get and print conflict count
    if (myargs.x):
        dbinfo.get_conflicts()
    
    # Get and print shard details
    if (myargs.s):
        dbinfo.get_node_list()
        

if __name__ == "__main__":
    main()

