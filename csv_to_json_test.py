#!/usr/bin/env python

import csv
import json

### Code import from example
#def main(argv):
#   inputfile = ''
#   outputfile = ''
#   try:
#      opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
#   except getopt.GetoptError:
#      print 'test.py -i <inputfile> -o <outputfile>'
#      sys.exit(2)
#   for opt, arg in opts:
#      if opt == '-h':
#         print 'test.py -i <inputfile> -o <outputfile>'
#         sys.exit()
#      elif opt in ("-i", "--ifile"):
#         inputfile = arg
#      elif opt in ("-o", "--ofile"):
#         outputfile = arg
#   print 'Input file is "', inputfile
#   print 'Output file is "', outputfile
#
#if __name__ == "__main__":
#   main(sys.argv[1:])
###

csvfile = open('file.csv', 'r')
jsonfile = open('file.json', 'w')

# Specifying the field names will import the first row as data but name the fields
# Remove "fieldnames" from the DictReader statement to use the first line as the
# field names themselves.
fieldnames = ("FirstName","LastName","IDNumber","Message")
reader = csv.DictReader( csvfile, fieldnames)

# Output the beginning of the over-arching JSON doc that will contain an array of docs
jsonfile.write('{ "docs":[')

rowcount = 0

for row in reader:
    # If this isn't the first document to write, lead a comma
    if rowcount != 0:
        jsonfile.write(',')
    else:
        rowcount = rowcount + 1
    
    # Output the current row as a JSON document
    json.dump(row, jsonfile)

# Write the final closing bracket for the "docs" document
jsonfile.write(']}')