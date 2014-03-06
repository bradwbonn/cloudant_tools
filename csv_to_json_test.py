#!/usr/bin/env python

import csv
import json

csvfile = open('file.csv', 'r')
jsonfile = open('file.json', 'w')
rowcounter = 0

fieldnames = ("FirstName","LastName","IDNumber","Message")
reader = csv.DictReader( csvfile, fieldnames)

# Output the beginning of the over-arching JSON doc that will contain an array of docs
jsonfile.write('{ "docs:[')

for row in reader:
    # If this isn't the first row, lead a comma
    if reader.line_num == 1:
        jsonfile.write('\n')
    else:
        jsonfile.write(',\n')
    
    # Output the current row as a JSON document and append a new line for readability
    json.dump(row, jsonfile)
    jsonfile.write('\n')

# Write the final closing bracket for the "docs" document
jsonfile.write(']\n}')