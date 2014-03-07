#!/usr/bin/env python

# csv2json.py
# Brad Bonn
# Cloudant, Inc.
# brad.bonn@cloudant.com
#
# This script imports a csv file and converts it into a JSON document file(s) that contains
# an array of JSON documents, each corresponding to one row of the CSV file.
#
# The output can be paired with a /_bulk_docs API call to import a large number of docs
# into a database in chunks, rather than one at a time.
#
# The script accepts the following parameters:
#  -i <csv file> (mandatory, CSV file to be read)
#  -o <json file> (optional, file to write, omitting creates file(s) with name "file.json")
#  -f <field names> (optional, if omitted, the first row of the CSV file will be considered
#     the field names. Use single quoted string with commas for the names.)
#  -s (optional, skips first row of the CSV file. Must be accompanied by -f)
#  -n <rows> (optional, creates separate output files with 'n' rows each. Using this option
#     causes '-o' to be ignored, and creates files with the name 'file_xxx.json'
#     with 'xxx' incrementing)

import csv
import json
import sys
import getopt

# Pre-defined help string to print upon '-h' or args mistake
help = 'csv2json.py -i <inputfile> [-o <outputfile>] [-f <field names>] [-s] [-n <rows per file>]'

def main(argv):
    
    #Argument variables
    inputfile = ''
    outputfile = ''
    fieldnames = ''
    rowsperfile = 0
    skipfirst = 0
    
    # Check options for validity, print help if user fat-fingered anything
    try:
        opts, args = getopt.getopt(argv,"hi:o:f:sn:")
    except getopt.GetoptError:
        print help
        sys.exit(2)
        
    # Parse options and set appropriate variables based on input
    for opt, arg in opts:
        if opt == '-h':
            print help
            sys.exit()
        elif opt in ("-i"):
            inputfile = arg
        elif opt in ("-o"):
            outputfile = arg
        elif opt in ("-f"):
            fieldnames = arg
        elif opt in ("-s"):
            skipfirst = 1
        elif opt in ("-n"):
            rowsperfile = arg
            # Do not allow negative numbers or 1 doc per file
            if (rowsperfile < 0 or rowsperfile == 1):
                print 'Minimum docs per file is 2'
                sys.exit(2)
    # Open input file
    if len(inputfile) < 1:
        print help
        sys.exit(2)
    
    try:
        csvfile = open(inputfile, 'r')
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        sys.exit(2)
    
    # If field names are pre-defined by arg, create dictreader using specified field names for columns
    # Otherwise, have dictreader use the first line as the field names for the columns. If user doesn't
    # specify the field names, script will IGNORE the request to skip the first line (-s)  
    if (fieldnames != ''):
        # Convert field names string to an array separated by commas and use as field names for reader
        reader = csv.DictReader( csvfile, fieldnames.split(','))
    else:
        reader = csv.DictReader( csvfile)
        skipfirst = 0
        
    # If no output file was specified and we're not breaking up into separate files, use "file.json"
    if (outputfile == '' and rowsperfile == 0):
        outputfile = 'file.json'
        createsinglejsonfile(reader,outputfile,skipfirst)
    # If output filename was specified and we're not breaking up into separate files, use that filename
    elif (outputfile != '' and rowsperfile == 0):
        createsinglejsonfile(reader,outputfile,skipfirst)
    # If the user wants separate files for each input, run multi-file function with row count
    elif (rowsperfile != 0):
        createjsonfiles(reader,rowsperfile,skipfirst)
        
    csvfile.close()
    print 'Fields used: ',reader.fieldnames
    
def createsinglejsonfile(csvdict,outputfilename,dontusefirstrow):
    try:
        jsonfile = open(outputfilename, 'w')
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        sys.exit(2)
    # Output the beginning of the over-arching JSON doc that will contain an array of docs
    jsonfile.write('{ "docs":[')

    rowcount = 0
    skipcheck = 0

    for row in csvdict:
        # If this is the first iteration, and we want to skip the first row, go to the next loop
        if (skipcheck == 0 and dontusefirstrow == 1):
            skipcheck = 1
            continue
        # If this isn't the first document to write, lead a comma
        if rowcount != 0:
            jsonfile.write(',')
            rowcount = rowcount + 1
        else:
            rowcount = rowcount + 1
    
        # Output the current row as a JSON document
        json.dump(row, jsonfile)

    # Write the final closing bracket for the "docs" document
    jsonfile.write(']}')
    jsonfile.close()
    print 'Total rows converted: ',rowcount
    pass
    
def createjsonfiles(csvdict,filelength,dontusefirstrow):
    # For every row, find the state. States can be:
    # 1) First row for a new file
    # 2) Row within a file
    # 3) Last row within a file
    
    rowcount = 1
    skipcheck = 0
    filecount = 0
    totalrows = 0
    
    for row in csvdict:
        currentfilename = 'file_%d.json' % filecount
        # Go to next loop if we're in the first iteration and we want to skip the first row
        if (skipcheck == 0 and dontusefirstrow == 1):
            skipcheck = 1
            continue
        # If this is the first row for a sequence, open the new JSON doc, write the beginning,
        # then output the row and increase rowcount
        if (rowcount == 1):
            try:
                jsonfile = open(currentfilename, 'w')
            except IOError as e:
                print "I/O error({0}): {1}".format(e.errno, e.strerror)
                sys.exit(2)
            jsonfile.write('{ "docs":[')
            json.dump(row, jsonfile)
            rowcount = rowcount + 1
            totalrows = totalrows + 1
        # If this is a row in the middle of a document, write a leading comma, then output
        # the row and increase rowcount
        elif ((rowcount > 1) and (int(rowcount) / int(filelength) != int(rowcount) / float(filelength))):
            jsonfile.write(',')
            rowcount = rowcount + 1
            json.dump(row, jsonfile)
            totalrows = totalrows + 1
        # If this is the last row for a document, output the comma, row, then write the final
        # closing bracket, close the file, then reset rowcount to 1 and increment filecount
        elif ((rowcount > 1) and (int(rowcount) / int(filelength) == int(rowcount) / float(filelength))):
            jsonfile.write(',')
            json.dump(row, jsonfile)
            jsonfile.write(']}')
            jsonfile.close()
            rowcount = 1
            filecount = filecount + 1
            totalrows = totalrows + 1
        else:
            print "That just don't add up! (row checking error)"
            sys.exit(2)
    # If the file handler is still open, append the final closing bracket and then close it
    if (jsonfile.closed):
        print 'Final file written: ' + currentfilename
    else:
        jsonfile.write(']}')
        jsonfile.close()
        print 'Final file written: ' + currentfilename
    print 'Total rows converted: ',totalrows
    pass

if __name__ == "__main__":
    main(sys.argv[1:])