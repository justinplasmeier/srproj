import csv
import os
import re

DATA_PATH = '/home/jgp/srproj/data'
data_files = os.listdir(DATA_PATH)
data_paths = [os.path.join(DATA_PATH, x) for x in data_files]
LOAD_PATH = '/home/jgp/load/data'
COUNT = 0

def get_count():
    with open('count.txt') as f:
        COUNT = int(f.read())
        print COUNT
    return COUNT

def set_count(count):
    f = open('count.txt', 'r+')
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
        return unicode("VARCHAR(10)")

    pattern = re.compile("^\-*\d*\.?\d*$")
    to_compare = unicode(value)
    if to_compare == unicode("PrivacySuppressed") or to_compare == unicode("NULL"):
        return "DECIMAL"
    elif re.match(pattern, to_compare):
        return "DECIMAL"
    return "VARCHAR(100)"

def extract_csv(filename):
    """
    Read the column headers and their inferred value into a dict.
    """
    schema_dict = {}

    with open(filename, 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        column_names = []
        data_type = [] 

        headers = spamreader.next()
        value_row = spamreader.next()
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
    f.write("CONSTRAINT persons_pkey PRIMARY KEY (UNITID) )")
    f.close()
    return filename


def apply_xform():
    """
    Appends the extra columns from the transform step to the schema.
    """
    pass


if __name__ == "__main__":
    COUNT = get_count()
    for data_file in data_files:
        data_path = os.path.join(DATA_PATH, data_file)
        load_path = os.path.join(LOAD_PATH, data_file)
        schema_dict = extract_csv(data_path)
        f = dump_to_csv(load_path[:-4] + 'schema.sql', schema_dict)
        print("wrote to:", f)
    set_count(COUNT)
