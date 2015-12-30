# Some helpful Cloudant tools I've written
## dbinfo.py
* Useful tool that can be used to obtain a large quantity of useful information about a Cloudant database
* Available data points include
  * Shard counts
  * Shard locations on cluster nodes - (makes db_shards.py redundant)
  * Size of data
  * Size of indexes
  * List of all indexes by types
  * Overhead from deleted document tombstones - (experimental)
  * Deleted document count
  * A count of all document conflicts in the database - (very slow, use with caution)
* Uses auth strings from environment variables
  * `CLOUDANT_ADMIN_AUTH='Basic: <authstring>'` (only needed for shard details)
  * `CLOUDANT_AUTH='Basic: <authstring>'`
* How to use: `dbinfo.py -u <user> -d <database> [-s [-k]] [-i] [-x] [-v]`
  * Always Mandatory Parameters:
    * `-u <Cloudant username>`
    * `-d <Cloudant database>`
  * Optional parameters:
    * `-s` Outputs a map of shard distributions on cluster nodes 
    * `-k` Includes a legend of shard distribution ranges (requires -s)
    * `-i` Prints a list of indexes and their associated sizes:
    * `-x` Scans database for total # of conflicts
    * `-v` (Be verbose)
  
## userdbs.py
* Lists all databases in the specified Cloudant account and their basic statistics in an easy-to-read format
* `userdbs.py -u <cloudant account>`
* Uses auth strings from environment variables
  * `CLOUDANT_ADMIN_AUTH='Basic: <authstring>'` or
  * `CLOUDANT_AUTH='Basic: <authstring>'`

## cluster_disk.py
* Admin tool that gives back the current disk usage on each node in a cluster, along with the change over the past 4 minutes
* Requires cluster admin rights
* `python cluster_disk.py -u <cloudant user> -c <cloudant cluster>`
* Uses auth string stored in environment variable
  * `CLOUDANT_ADMIN_AUTH='Basic: <authstring>'`

## csv2json.py
* Script that converts a CSV file into one (or more) JSON files
* Useful if you want to dump data into Cloudant using the _bulk_docs API from curl or Postman
 
### db_shards.py DEPRECATED: replaced by dbinfo.py