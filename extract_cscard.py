import sys, os
sys.path.insert(0, os.path.abspath('..'))

import geocivics_assets
from collections import OrderedDict
import csv
import os
import re

DATA_PATH = '/home/jgp/geocivics/srproj/data'
data_files = os.listdir(DATA_PATH)
data_paths = [os.path.join(DATA_PATH, x) for x in data_files]
LOAD_PATH = '/home/jgp/geocivics/load/data'
COUNT = 0

def get_count():
    with open('/home/jgp/geocivics/srproj/count.txt') as f:
        COUNT = int(f.read())
        print(COUNT)
    return COUNT

def set_count(count):
    f = open('/home/jgp/geocivics/srproj/count.txt', 'r+')
    f.truncate()
    value = int(count)+1
    text = str(value)
    f.write(text)
    f.close()

def guess_data_type(header, value):
    """
    Takes a value and guesses its type.
    """
    if header == 'ZIP':
        return "VARCHAR(10)"

    pattern = re.compile("^\-*\d*\.?\d*$")
    to_compare = value
    if to_compare == "PrivacySuppressed" or to_compare == "NULL":
        return "DECIMAL"
    elif re.match(pattern, to_compare):
        return "DECIMAL"
    return "VARCHAR(1000)"

def extract_csv(filename):
    """
    Read the column headers and their inferred value into a dict.
    """
    schema_dict = OrderedDict() 

    with open(filename, 'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        column_names = []
        data_type = [] 

        headers = next(spamreader)
        value_row = next(spamreader)
        for col_name, value in zip(headers, value_row):
            schema_dict[col_name] = guess_data_type(col_name, value)
    print("{} keys".format(len(schema_dict)))
    return schema_dict

def dump_to_csv(filename, schema_dict):
    f = open(filename, 'w')
    f.truncate()
    f.write("CREATE TABLE college_scorecard_"+str(COUNT)+ " ( \n")
    for key in schema_dict.keys():
        feature = "  %s %s,\n"%(key,schema_dict[key])
        f.write(feature)
    f.write("CONSTRAINT persons_pkey_"+str(COUNT)+" PRIMARY KEY (UNITID) )")
    f.close()
    return filename

def fix_csv(filename):
    reader = csv.reader(open(filename, "r+"))
    for row in reader:
        for i, x in enumerate(row):
            if len(x)< 1:
                x = row[i] = "NULL"

if __name__ == "__main__":
    COUNT = get_count()
    asset_list = geocivics_assets.get_assets()
    for asset in asset_list:
        schema_dict = extract_csv(asset.data_path)
        fix_csv(asset.data_path)
        for xform in asset.transformations:
            schema_dict[xform.column_name] = xform.column_type
            print("added: " + xform.column_name + " type " + xform.column_type)
        f = dump_to_csv(asset.load_path[:-4] + 'schema.sql', schema_dict)
        print("wrote to:", f)
    set_count(COUNT)
