#!/usr/bin/env python

# Batch export of all docs from a Cloudant DB to a CSV file in Amazon S3
# Takes stored environment variables for Cloudant auth data
# Assumes S3 auth is configurd in local file by AWS CLI
# Maximum nested data depth is hard-coded to 4. Look at marked section to modify

# Author: Brad Bonn - bbonn@us.ibm.com

import logging, os, json, csv, sys, re, argparse
import boto3, botocore
from pprint import pprint
from contextlib import closing

schema_map = dict()

def configuration():
    argparser = argparse.ArgumentParser(
        description = 'Tool to export JSON data from Cloudant into flat CSV files in Amazon S3\nManage S3 creds with AWS tool, Cloudant creds in environment variables.'
    )
    argparser.add_argument(
        'account',
        type=str,
        help = 'Cloudant account name'
    )
    argparser.add_argument(
        'database',
        type=str,
        help = 'Cloudant database name'
    )
    argparser.add_argument(
        'schema',
        type = file,
        help = "JSON file which contains the schema map to use to build the CSV files from."
    )
    argparser.add_argument(
        'bucket',
        type=str,
        help='S3 bucket for uploading to.'
    )
    argparser.add_argument(
        '-f',
        metavar = 'folder_name',
        type=str,
        nargs='?',
        help='S3 folder for uploading to. Defaults to cloudant_export.',
        default='cloudant_export'
    )
    argparser.add_argument(
        '-u',
        action = 'store_true',
        help="Output a signed URL for the CSV object once it's uploaded."
    )
    argparser.add_argument(
        '--cloudant_user',
        metavar = 'username',
        type=str,
        nargs='?',
        help = 'Alternate Cloudant username for auth. Defaults to environment variable.',
        default = os.environ.get('CLOUDANT_USER')
    )
    argparser.add_argument(
        '--cloudant_pass',
        metavar = 'password',
        type=str,
        nargs='?',
        help = 'Alternate Cloudant password for auth. Defaults to environment variable.',
        default = os.environ.get('CLOUDANT_PASS')
    )
    argparser.add_argument(
        '-d',
        metavar = 'delimeter',
        type=str,
        nargs='?',
        help = 'Set the delimeter for each field in a row. Defaults to a comma.',
        default = ','
    )
    argparser.add_argument(
        '--region',
        metavar = 's3_region',
        type=str,
        nargs='?',
        help = 'Set the s3 region to upload to. Defaults to us-east-1.',    
        default = 'us-east-1'
    )
    myargs = argparser.parse_args()
    
    return myargs

def main():
    # Process arguments and get configuration settings
    myargs = configuration()
    
    # Open log file
    try:
        logging.basicConfig(filename='ctos3.log', level=30)
        logging.captureWarnings(True)
    except Exception:
        print "Can't open local log file. JSON encoding warnings will not be recorded."
    
    # Set the S3 key (filename)
    s3key = "{0}/{1}.csv".format(myargs.f,myargs.database)
    
    # Populate schema_map from JSON file
    try:
        schema_map = json.loads(myargs.schema)
    except Exception as e:
        print "Unable to read schema map JSON file: {0}".format(e)
        
    # Check that the bucket exists
    #s3client = boto3.client('s3', myargs.region)
    #bucket = s3.Bucket(myargs.bucket)
    #try:
    #    s3.meta.client.head_bucket(Bucket=myargs.bucket)
    #except botocore.exceptions.ClientError as e:
    #    # If a client error is thrown, then check that it was a 404 error.
    #    # If it was a 404 error, then the bucket does not exist.
    #    error_code = int(e.response['Error']['Code'])
    #    if error_code == 404:
    #        sys.exit("Invalid s3 bucket: {0}".format(myargs.bucket))
    #  
    # Fire up generator to iterate through all_docs in a stream
    generator = csvGenerator(myargs)
    
    # Use the generator as the data stream to upload the CSV object to S3
    s3 = boto3.client('s3')
    
    s3.upload_file(generator, myargs.bucket, s3key)

    # Generate the URL to get 'key-name' from 'bucket-name'
    if myargs.u:
        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': myargs.bucket,
                'Key': s3key
            }
        )
        print "Presigned URL for uploaded file: \n{0}".format(url)
    
    
# This is the hard-coded segment for nested JSON data.  Alter it for increased depth handling.
def get_field(json_doc, field):
    try:
        if len(field) == 1:
            data = json_doc[field[0]]
        elif len(field) == 2:
            data = json_doc[field[0]][field[1]]
        elif len(field) == 3:
            data = json_doc[field[0]][field[1]][field[2]]
        elif len(field) == 4:
            data = json_doc[field[0]][field[1]][field[2]][field[3]]
        else: # Error in schema map format
            sys.exit("{0}\n{1}\n{2}".format(
                "Schema file formatting error.",
                "Format should be:",
                "{ 'Header1': ['json_field'], 'Header2': ['json_field','nested_json_field'], ...}"
                )
            )
    except:
        data = '' # Can't get the field from this document
    return data
    
def csvGenerator(a):
    # For first output, CSV row of headers
    yield a.d.join(schema_map.keys())
    
    url_format = 'https://{0}.cloudant.com/{1}/_all_docs?include_docs=true&limit=100' #testing on 100 docs only
    url = url_format.format(
        a.account,
        a.database
    )
    with closing(requests.get(url, stream=True, auth=HTTPBasicAuth(a.username, a.password))) as r:
        for line in r.iter_lines:
            # remove a trailing comma, if any
            text = re.sub(r',$', '', line)
                
            # Convert to JSON
            try:
                json_line = json.loads(text)
                json_doc = json_line['doc']
            except Exception as e:
                logging.warn("JSON enocde error: {0}".format(e))
                continue
            
            # extract fields via schema_map
            extracted_fields = []
            for field in schema_map.values():
                extracted_fields.append(get_field(json_doc,field))
            yield a.d.join(str(i) for i in extracted_fields) + "\n"
            del extracted_fields[:]

def pretty_size(size):
    measure = 0
    size = float(size)
    while (size > 1024):
        size = round(size / 1024, 2)
        measure = measure + 1
    codes = [' b',' K',' M',' G',' T',' P']
    if measure == 0:
        size = int(size)
    formattedsize = "{:,}".format(size)
    return (formattedsize + codes[measure])  

if __name__ == '__main__':
    main()