#!/usr/bin/env python
import requests
import json
import sys
import argparse
import os
import string
import locale
from multiprocessing import Pool
import numpy as np
import time

config = dict(
    my_header = dict(),
    account = '',
    maxdbs = 40,
    summary_only = False,
    force_list = False,  
    totals = dict(
        shardcount = 0,
        active = 0,
        disk = 0,
        doc_count = 0,
        del_doc_count = 0,
        nvalue = 0
    )
)

def main(argv):
    
    argparser = argparse.ArgumentParser(description = '')
    argparser.add_argument(
        'account',
        type=str,
        help='Cloudant DBaaS account name (https://<account>.cloudant.com)'
        )
    argparser.add_argument(
        '-f',
        help = 'Force list of databases, even if there are over {0}'.format(config['maxdbs']),
        action = 'store_true'
        )
    myargs = argparser.parse_args()
    config['account'] = myargs.account
    config['force_list'] = myargs.f

    # Set authentication up        
    adminauthstring = os.environ.get('CLOUDANT_ADMIN_AUTH')
    authstring = os.environ.get('CLOUDANT_AUTH')
    if len(adminauthstring) > 0:
        authstring = adminauthstring
    elif len(authstring) == 0:
        sys.exit("ERROR: Required environment variables not set")
    config['my_header'] = {'Content-Type': 'application/json', 'Authorization': authstring}
    
    # Get list of databases for the account
    myurl = 'https://{0}.cloudant.com/_all_dbs'.format(config['account'])
    dbs = http_request(myurl)
    config['dbcount'] = len(dbs)
    
    # If the total number of databases is greater than 40 and user forces details
    if (config['dbcount'] > config['maxdbs']) and config['force_list']:
        give_estimate(dbs,True)
        detail_table(dbs)
        
    # If dbcount > 40 and user doesn't force, build and print a summary    
    elif (config['dbcount'] > config['maxdbs']) and not config['force_list']:
        give_estimate(dbs,False)
        summary(dbs)
        
    # Otherwise, default to printing detail table    
    else:
        detail_table(dbs)

def detail_table(dbs):
    # Figure out width of db name field
    dblen = 10
    for db in dbs:
        if len(db) > dblen:
            dblen = len(db)
            
    # Define formatting for table
    headerformat = "|{0:^" + str(dblen) + "}|{1:^4}|{2:^3}|{3:^10}|{4:^11}|{5:^14} |{6:^14} |"
    summaryline = "|{0:" + str(dblen) + "}|{1:^4}|{2:^3}|{3:>10}|{4:>11}|{5:>14} |{6:>14} |"    
    headline = headerformat.format(
        'Database',
        'Q',
        'N',
        'Active',
        'Disk',
        'Docs',
        'Deleted Docs'
    )
    width = len(headline)
    
    # Open a multi-process pool with CPU count processes
    p = Pool()
    # Get details of each database
    results_array = p.map(get_details, dbs)

    # Begin printing table    
    print "_" * width
    
    print headline
    
    print "-" * width    

    # Print each database's details and add to totals
    for result in results_array:
        print summaryline.format(
            result['db'],
            result['shardcount'],
            result['nvalue'],
            data_size_pretty(result['active']),
            data_size_pretty(result['disk']),
            count_pretty(result['doc_count']),
            count_pretty(result['del_doc_count'])
        )
        
        for key, value in result.iteritems():
            if key != 'db':
                config['totals'][key] = config['totals'][key] + value
      
    print "-" * width
    
    print summaryline.format(
        "Totals:",
        config['totals']['shardcount'], 
        'N/A',
        data_size_pretty(config['totals']['active']),
        data_size_pretty(config['totals']['disk']),
        count_pretty(config['totals']['doc_count']),
        count_pretty(config['totals']['del_doc_count'])
    )
    
    print "-" * width

def give_estimate(dbs, detail):
    p = Pool()
    # Sample set is the first 'maxdbs' of databases
    sub_array = dbs[0:config['maxdbs']]
    
    start_time = time.time()
    if detail:
        discard = p.map(get_details,sub_array)    
    else:
        discard = p.map(get_basic,sub_array)
    end_time = time.time()
    
    # Cleanup
    del discard
    del p
    
    est_time = pretty_time((end_time - start_time) * (config['dbcount'] / config['maxdbs']))
    
    # Print a time estimate for details, proceed when ready
    print " There are {0} databases in the account.".format(count_pretty(config['dbcount']))
    print " Estimated completion time: {0}\n".format(est_time)
    ready = raw_input(" Are you sure? (Y/n) ")
    if ready in ('n','N'):
        sys.exit(" Aborting.")


def summary(dbs):
    totals = np.array([0,0,0,0])
    totalsline = "|{0:20}|{1:>18} |"
    width = len(totalsline.format('',''))
    # Open a multi-process pool with CPU count processes
    p = Pool()
    
    start_time = time.time()
    totals_array = p.map(get_basic,dbs)
    end_time = time.time()
    
    print " HTTP Queries completed in: {0}".format(
        pretty_time((end_time - start_time))
    )
            
    # Cleanup
    del p
            
    # Sub all totals from array of result arrays
    # (This would be eliminated as a need if inter-process communication was implemented)
    for thisdb in totals_array:
        totals = totals + np.array(thisdb)
    
    print '_' * width
    print "|{0:^20}|{1:^18} |".format("Cloudant Account:",config['account'])
    print '-' * width
    print totalsline.format("Number of databases",config['dbcount'])
    print totalsline.format("Total docs",count_pretty(totals[0]))
    print totalsline.format("Total deleted docs",count_pretty(totals[1]))
    print totalsline.format("Total active size",data_size_pretty(totals[2]))
    print totalsline.format("Total disk size",data_size_pretty(totals[3]))
    print '-' * width

def get_basic(db):
    myurl = 'https://{0}.cloudant.com/{1}'.format(config['account'],db)
    stats = http_request(myurl)
    # Account for small or empty databases, where the API gets weird on disk space
    if stats['sizes']['active'] == None:
        active = stats['sizes']['external']
    else:
        active = stats['sizes']['active']
    return [
        stats['doc_count'],
        stats['doc_del_count'],
        active,
        stats['sizes']['file']
    ]

def get_details(db):
    myurl = 'https://{0}.cloudant.com/{1}'.format(config['account'],db)
    stats = http_request(myurl)

    myurl = 'https://{0}.cloudant.com/{1}/_shards'.format(config['account'],db)
    shards = http_request(myurl)['shards']
    shardcount = len(shards)
    
    nvalue = len(shards.itervalues().next())
    doc_count = int(stats['doc_count'])
    del_doc_count = int(stats['doc_del_count'])
    disk = stats['sizes']['file']
    
    # Account for small or empty databases, where the API gets weird on disk space
    if stats['sizes']['active'] == None:
        active = stats['sizes']['external']
    else:
        active = stats['sizes']['active']

    return dict(
        db = db,
        shardcount = shardcount,
        nvalue = nvalue,
        active = active,
        disk = disk,
        doc_count = doc_count,
        del_doc_count = del_doc_count
    )

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

def count_pretty(count):
    return locale.format("%d", count, grouping=True)
    #return "{:,}".format(count)
    
def http_request(url):
    r = requests.get(
        url,
        headers = config['my_header']
    )
    if r.status_code not in (200,201,202):
        sys.exit("Failed, bad HTTP response")
    return r.json()

if __name__ == "__main__":
    locale.setlocale(locale.LC_ALL, 'en_US')
    main(sys.argv[1:])

