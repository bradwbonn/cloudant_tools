# Some helpful Cloudant tools I've written
### cluster_disk.py
* Gives back the current disk usage on each node in a cluster, along with the change over the past 4 minutes
* Uses the monitoring API endpoint for a Cloudant cluster.  Requires admin rights
* `python cluster_disk.py -u <cloudant user> -c <cloudant cluster>`
* Uses auth string stored in environment variable
 

### csv2json.py
* Script that converts a CSV file into one (or more) JSON files
* Useful if you want to dump data into Cloudant using the _bulk_docs API from curl or Postman
