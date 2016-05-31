#!/usr/bin/env python

import argparse, requests, json, os, sys

def getargs():
    argparser = argparse.ArgumentParser(description = 'Display the status of all tasks running on a Cloudant account')
    argparser.add_argument(
        'account',
        type=str,
        help='Cloudant DBaaS account name (https://<account>.cloudant.com)'
        )
    argparser.add_argument(
        '-d',
        action='store_true',
        help='Display detailed information about tasks'
    )
    return argparser.parse_args()

def main():
    activetasks = CloudantActiveTasks(getargs())
    activetasks.get()

class CloudantActiveTasks(object):
    
    def __init__(self, args):
        self.account = args.account
        self.detail = args.d
        adminauthstring = os.environ.get('CLOUDANT_ADMIN_AUTH')
        authstring = os.environ.get('CLOUDANT_AUTH')
        if len(adminauthstring) > 0:
            authstring = adminauthstring
        elif len(authstring) == 0:
            sys.exit("ERROR: Required environment variables not set")
        self.my_header = {'Content-Type': 'application/json', 'Authorization': authstring}
        self.tasks_raw = []
        self.types = dict(
            indexer = 0,
            replication = 0,
            search_indexer = 0,
            view_compaction = 0,
            database_compaction = 0,
        )
        
    def get(self):
        myurl = 'https://{0}.cloudant.com/_active_tasks'.format(self.account)
        self.tasks_raw = self.json_get(myurl)
        print " Active tasks for account " + self.account
        for task in self.tasks_raw:
            if task['type'] == 'replication':
                pending = task['changes_pending']
            else:
                pending = task['total_changes'] - task['changes_done']
                if self.detail:
                    print " Type: {0} - Pending changes: {1}".format(
                        task['type'],
                        pending
                    )
            self.types[task['type']] = self.types[task['type']] + pending
        print " Total changes left:"
        for summary in self.types.keys():
            if self.types[summary] > 0:
                print " {0}: {1}".format(summary, self.types[summary])
    
    def json_get(self, url):
        r = requests.get(
            url,
            headers = self.my_header
        )
        if r.status_code not in (200,201,202):
            sys.exit("Failed, bad HTTP response")
        return r.json()
    
if __name__ == "__main__":
    main()
