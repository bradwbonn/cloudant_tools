#!/usr/bin/env python
import requests
import json
import sys
import getopt
import os
import string
import locale
from multiprocessing import Pool

config = dict(
    my_header = dict(),
    account = '',
    dblen = 10,
    width = 107-40,
    results = dict()
)

def main(argv):
    locale.setlocale(locale.LC_ALL, 'en_US')

    try:
        opts, args = getopt.getopt(argv,"u:")
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-u':
            config['account'] = arg

    # Set authentication up        
    adminauthstring = os.environ.get('CLOUDANT_ADMIN_AUTH')
    authstring = os.environ.get('CLOUDANT_AUTH')
    if len(adminauthstring) > 0:
        authstring = adminauthstring
    elif len(authstring) == 0:
        sys.exit("ERROR: Required environment variables not set")
    config['my_header'] = {'Content-Type': 'application/json', 'Authorization': authstring}
    
    # Get list of databases for the account
    dbs = get_all_dbs()
    
    # Figure out the print format column width dynamically
    for db in dbs:
        if len(db) > config['dblen']:
            config['dblen'] = len(db)
    config['width'] = config['width'] + config['dblen']
    
    print_headerline()
    
    # Spawn a thread for each CPU
    # Each thread pulls and prints the stats of its passed database
    p = Pool()
    results_array = p.map(get_summary, dbs)

    for result in results_array:
        config['results'][result['db']] = dict(
            shardcount = result['shardcount'],
            nvalue = result['nvalue'],
            active = result['active'],
            disk = result['disk'],
            doc_count = result['doc_count'],
            del_doc_count = result['del_doc_count']
        )
    
    print_db_details()
    
    print "-" * config['width']

def print_headerline():
    headerline = "|{0:^" + str(config['dblen']) + "}|{1:^4}|{2:^3}|{3:^10} |{4:^10} |{5:^14} |{6:^14} |"
    print "_" * config['width']
    print headerline.format(
            'Database',
            'Q',
            'N',
            'Active',
            'Disk',
            'Docs',
            'Deleted'
        )
    print "-" * config['width']

def print_db_details():
    summaryline = "|{0:" + str(config['dblen']) + "}|{1:^4}|{2:^3}|{3:>10} |{4:>10} |{5:>14} |{6:>14} |"
    for database in sorted(config['results']):
        print summaryline.format(
            database,
            config['results'][database]['shardcount'],
            config['results'][database]['nvalue'],
            config['results'][database]['active'],
            config['results'][database]['disk'],
            config['results'][database]['doc_count'],
            config['results'][database]['del_doc_count']
        )

def get_summary(db):
    myurl = 'https://{0}.cloudant.com/{1}'.format(config['account'],db)
    r = requests.get(
        myurl,
        headers = config['my_header']
    )
    if r.status_code not in (200,201,202):
        sys.exit("Failed, bad HTTP response")
    stats = r.json()
    if (stats['doc_count'] == 0):
        print summaryline.format(db,'-','-',0,0,0,'-')
        return
    shards = get_shard_count(config['account'],db)
    shardcount = len(shards)
    nvalue = len(shards.itervalues().next())
    doc_count = count_pretty(int(stats['doc_count']))
    del_doc_count = count_pretty(int(stats['doc_del_count']))
    disk = data_size_pretty(stats['sizes']['file'])
    # Account for small or empty databases, where the API gets weird on disk space
    if stats['sizes']['active'] == None:
        active = data_size_pretty(stats['sizes']['external'])
    else:
        active = data_size_pretty(stats['sizes']['active'])
    #print summaryline.format(db,shardcount,nvalue,active,disk,doc_count,del_doc_count)
    return dict(
        db = db,
        shardcount = shardcount,
        nvalue = nvalue,
        active = active,
        disk = disk,
        doc_count = doc_count,
        del_doc_count = del_doc_count
    )
    
def get_shard_count(account, dbname):
    myurl = 'https://{0}.cloudant.com/{1}/_shards'.format(account,dbname)
    r = requests.get(
        myurl,
        headers = config['my_header']
    )
    if r.status_code not in (200,201,202):
        sys.exit("Failed, bad HTTP response")
    json_response = r.json()
    return json_response['shards']

def get_all_dbs():
    myurl = 'https://{0}.cloudant.com/_all_dbs'.format(config['account'])
    r = requests.get(
        myurl,
        headers = config['my_header']
    )
    if r.status_code not in (200,201,202):
        sys.exit("Failed, bad HTTP response")
    json_response = r.json()
    return json_response

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

def count_pretty(count):
    return locale.format("%d", count, grouping=True)
    #return "{:,}".format(count)

if __name__ == "__main__":
    main(sys.argv[1:])

