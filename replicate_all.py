#!/usr/bin/env python

# This script will replicate all databases in a Cloudant account to another Cloudant account.
# Use this for duplicating the entire content of an account to another location for testing purposes

import requests,json,argparse,sys,getpass,os
from base64 import b64encode

from pprint import pprint


config = dict(
    header = dict(),
    sourceheader = '',
    destpass = '',
    sourcepass = ''
)

def main():
    myargs = get_args()
    set_auth(myargs)
    if myargs.t == False:
        dblist = get_dbs(myargs)
        repcount = 0
        for db in dblist:
            if userdb(db):
                replicate(db, myargs)
                repcount = repcount + 1
        print "{0} replication documents inserted. Use _active_tasks to check status.".format(repcount)
        print "To signal stop of replication tasks, run script again with -t option."
    else:
        terminate_tasks(myargs)
    
def get_args():
    argparser = argparse.ArgumentParser(description = 'Replicate all databases in one account to another account.\nAll replication tasks with have the name "alldbs-<dbname>"')
    argparser.add_argument(
        'source',
        type=str,
        help='Cloudant DBaaS source account name (e.g. https://<username>.cloudant.com)'
        )
    argparser.add_argument(
        'destination',
        type=str,
        help='Cloudant DBaaS destination account name (e.g. https://<username>.cloudant.com)'
        )
    argparser.add_argument(
        '-c',
        help = 'Make replication tasks continuous',
        action = 'store_true'
        )
    argparser.add_argument(
        '-e',
        help = 'Only replicate databases that already exist at destination (create_target = false)',
        action = 'store_true'
        )
    argparser.add_argument(
        '-t',
        help = 'Terminate *ALL* replication tasks started by this script\n(Deletes all docs in _replicator starting with alldbs-*)',
        action = 'store_true'
    )
    return(argparser.parse_args())

def userdb(db):
    # Filter out reserved databases
    if db[0] == '_':
        return False
    else:
        return True

def replicate(db, myargs):
    repdoc = {
        "source": "https://{0}:{1}@{2}.cloudant.com/{3}".format(
            myargs.source,
            config['sourcepass'],
            myargs.source,
            db
        ),
        "target": "https://{0}:{1}@{2}.cloudant.com/{3}".format(
            myargs.destination,
            config['destpass'],
            myargs.destination,
            db
        ),
        "create_target": not myargs.e,
        "continuous": myargs.c,
        "name": "alldbs-{0}".format(db),
        "_id": "alldbs-{0}".format(db)
    }
    pprint(repdoc)
    repdburl = "https://{0}.cloudant.com/_replicator".format(myargs.source)
    http_post(repdburl, config['sourceheader'], json.dumps(repdoc))

def set_auth(myargs):
    print "Enter password for '{0}' account".format(myargs.source)
    config['sourcepass'] = getpass.getpass()
    sourceauth = b64encode("{0}:{1}".format(myargs.source,config['sourcepass']))
    config['sourceheader'] = {
        'Content-Type': 'application/json',
        'Authorization': "Basic {0}".format(sourceauth)
    }
    print "Enter password for '{0}' account".format(myargs.destination)
    config['destpass'] = getpass.getpass()
    
def get_dbs(myargs):
    myurl = "https://{0}.cloudant.com/_all_dbs".format(myargs.source)
    dbjson = http_get(myurl, config['sourceheader'])
    print "Total databases in account: {0}".format(len(dbjson))
    return(dbjson)

def http_get(url, header):
    try:
        r = requests.get(
            url,
            headers = header
        )
    except Exception as e:
        sys.exit("HTTP GET failed: {0}\nURL: {1}".format(e, url))    
    if r.status_code not in (200,201,202):
        sys.exit("HTTP GET failed, code: {0}\nURL: {1}".format(r.status_code, url))
    return r.json()

def http_delete(url, header):
    try:
        r = requests.delete(
            url,
            headers = header
        )
    except Exception as e:
        sys.exit("HTTP DELETE failed: {0}\nURL: {1}".format(e, url))    
    if r.status_code not in (200,201,202):
        sys.exit("HTTP DELETE failed, code: {0}".format(r.status_code))
    return r.json()
    
def http_post(url, header, content):
    try:
        r = requests.post(
            url,
            headers = header,
            data = content
        )
    except Exception as e:
        sys.exit("HTTP PUT failed: {0}\nURL: {1}".format(e, url))
    if r.status_code not in (200,201,202):
        sys.exit("HTTP PUT failed, code: {0}".format(r.status_code))
        
def terminate_tasks(myargs):
    import re
    # Get all docs in _replicator
    myurl = "https://{0}.cloudant.com/_replicator/_all_docs".format(myargs.source)
    repdocs = http_get(myurl, config['sourceheader'])
    for repdoc in repdocs['rows']:
        # if it's one of this script's replication docs, delete it
        m = re.search('alldbs-', repdoc['id'])
        if m:
            myurl = "https://{0}.cloudant.com/_replicator/{1}?rev={2}".format(
                myargs.source,
                repdoc['id'],
                repdoc['value']['rev']
            )
            http_delete(myurl, config['sourceheader'])
            print "Deleted _replicator/{0}".format(repdoc['id'])

if __name__ == "__main__":
    main()