# Some helpful Cloudant tools I've written
### cluster_disk.py
* Admin tool that gives back the current disk usage on each node in a cluster, along with the change over the past 4 minutes
* Uses the monitoring API endpoint for a Cloudant cluster.  Requires admin rights
* `python cluster_disk.py -u <cloudant user> -c <cloudant cluster>`
* Uses auth string stored in environment variable `CLOUDANT_ADMIN_AUTH='Basic: <authstring>'`
 
### db_shards.py
* Admin tool that shows the shard state of a database
* Can print a table of unique shards or the location of various shards throughout the nodes of a cluster
* `python db_shards.py -u <cloudant user> -c <cloudant cluster> -d <database> [-k]`
* optional `-k` will display which shard ranges correspond to which codes
* Not including `-k` automatically prints the shard distribution map and count of shards on each node for the database in question.
* Also uses auth string from environment variable like cluster_disk.py

### csv2json.py
* Script that converts a CSV file into one (or more) JSON files
* Useful if you want to dump data into Cloudant using the _bulk_docs API from curl or Postman
