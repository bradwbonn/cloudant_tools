# Some helpful Cloudant tools I've written

#### Almost all of these scripts use auth strings from the following environment variables
* `CLOUDANT_ADMIN_AUTH='Basic: <authstring>'` (only needed for cluster-level details)
* `CLOUDANT_AUTH='Basic: <authstring>'`

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
* Usage: `dbinfo.py <account> <database> [-s] [-i] [-x] [-v]`
  * Optional parameters:
    * `-s` Outputs a map of shard distributions on cluster nodes 
    * `-i` Prints a list of indexes and their associated sizes:
    * `-x` Scans database for total # of conflicts
    * `-v` Verbose output. For example, adds view names under each design document listed with `-i`
  
## userdbs.py
* Lists all databases in the specified Cloudant account and their basic statistics in an easy-to-read format
* Usage: `userdbs.py <cloudant account>`

## tasks.py
* Summarizes state of _active_tasks endpoint, giving an easy-to-read state of replication, indexing and compaction
* Usage: `tasks.py [-d] <account>`
  * Optional parameters:
    * `-d` Show detailed information about every task

## cluster_disk.py
* Admin tool that gives back the current disk usage on each node in a cluster, along with the change over the past 4 minutes
* Requires cluster admin rights
* Usage: `python cluster_disk.py <cloudant cluster>`
  
## replicate_all.py
* Replicates all databases from one account to another
* Helpful for duplicating an entire account's content to a test account, or for setting up a continuous replication task for a hot mirror.
* Only replicates one direction.  Bi-direction replication can be implemented by running the command again with source and destination reversed.
* Usage: `replicate_all.py [-t] [-c] [-e] <source> <destination>`
	* Optional parameters:
		* `-c` Make replication tasks continuous
		* `-e` Only replicate existing databases on target
		* `-t` Remove all script-created replication tasks to terminate ongoing replication

## csv2json.py
* Script that converts a CSV file into one (or more) JSON files
* Useful if you want to dump data into Cloudant using the _bulk_docs API from curl or Postman
 
