#!/usr/bin/env python

import csv
import json

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