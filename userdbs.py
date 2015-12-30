#!/usr/bin/env python
import requests
import json
import sys
import getopt
import os
import string
import locale

config = dict(
    my_header = dict()
)

def main(argv):
    width = 107
    headerline = "|{0:^40}|{1:^4}|{2:^3}|{3:^10} |{4:^10} |{5:^14} |{6:^14} |"
    locale.setlocale(locale.LC_ALL, 'en_US')
    print "_" * width
    print headerline.format(
            'Database',
            'Q',
            'N',
            'Active',
            'Disk',
            'Docs',
            'Deleted'
        )
    print "-" * width
    try:
        opts, args = getopt.getopt(argv,"u:")
    except getopt.GetoptError:
        sys.exit(2)    
    for opt, arg in opts:
        if opt == '-u':
            account = arg

    # Set authentication up        
    adminauthstring = os.environ.get('CLOUDANT_ADMIN_AUTH')
    authstring = os.environ.get('CLOUDANT_AUTH')
    if len(adminauthstring) > 0:
        authstring = adminauthstring
    elif len(authstring) == 0:
        sys.exit("ERROR: Required environment variables not set")
    config['my_header'] = {'Content-Type': 'application/json', 'Authorization': authstring}
    dbs = get_all_dbs(account)
    for database in dbs:
        print_summary(account, database)
    print "-" * width

def print_summary(account, db):
    myurl = 'https://{0}.cloudant.com/{1}'.format(account,db) 
    summaryline = "|{0:40}|{1:^4}|{2:^3}|{3:>10} |{4:>10} |{5:>14} |{6:>14} |"
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
    shards = get_shard_count(account,db)
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
    print summaryline.format(db,shardcount,nvalue,active,disk,doc_count,del_doc_count)
    
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

def get_all_dbs(account):
    myurl = 'https://{0}.cloudant.com/_all_dbs'.format(account)
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

