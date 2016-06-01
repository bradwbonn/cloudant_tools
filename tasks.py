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
        self.grouped_tasks = dict(
            replication = dict(),
            indexer = dict(),
            search_indexer = dict(),
            view_compaction = dict(),
            database_compaction = dict()
            )
        
    def get_new(self): # unfinished
        for task in self.tasks_raw:
            thistype = task['type']
            if thistype == 'indexer':
                shards,shard_range,username,database_and_time = task['database'].split('/')
                ddoc = task['design_document'].split('/')[1]
                pending = task['total_changes'] - task['changes_done']
                self.append_task(
                    [
                        thistype,
                        database_and_time.split('.')[0],
                        ddoc,
                        shard_range,
                        pending
                    ]
                )
            elif thistype == 'search_indexer':
                pass
            elif thistype == 'replication':
                pass
            elif thistype == 'view_compaction':
                pass
            elif thistype == 'database_compaction':
                pass
            
    
    def get(self):
        myurl = 'https://{0}.cloudant.com/_active_tasks'.format(self.account)
        self.tasks_raw = self.json_get(myurl)
        print " Active tasks for account " + self.account
        for task in self.tasks_raw:
            if task['type'] == 'replication':
                pending = task['changes_pending']
                if task['continuous']:
                    cont = 'Continuous'
                else:
                    cont = 'One-time'
                if self.detail:
                    print " {0} replication: {1}\n   Pending: {2}".format(
                        cont,
                        task['replication_id'],
                        pending
                    )
            elif 'indexer' in task['type']:    
                shards,shard_range,username,database_and_time = task['database'].split('/')
                ddoc = task['design_document'].split('/')[1]
                pending = task['total_changes'] - task['changes_done']
                if self.detail:
                    print " {0} {1} {2} {3} {4}%".format(
                        database_and_time.split('.')[0],
                        shard_range[:4],
                        task['type'],
                        ddoc,
                        task['progress']
                    )
            else:
                shards,shard_range,username,database_and_time = task['database'].split('/')
                pending = task['total_changes'] - task['changes_done']
                if self.detail:
                    print " {0} {1} {2} Pending: {3}".format(
                        database_and_time.split('.')[0],
                        shard_range[:4],
                        task['type'],
                        pending
                    )
            
            self.types[task['type']] = self.types[task['type']] + pending
        print " Total changes left:"
        for summary in self.types.keys():
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
