import csv
import os
import re

DATA_PATH = '/home/jgp/srproj/data'
data_files = os.listdir(DATA_PATH)
data_paths = [os.path.join(DATA_PATH, x) for x in data_files]

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

    f = open('schema_cscard.sql', 'w')
    f.truncate()
    f.write("CREATE TABLE college_scorecard ( \n")
    for key in schema_dict.keys():
        feature = "  %s %s,\n"%(key,schema_dict[key])
        f.write(feature)
    f.write("CONSTRAINT persons_pkey PRIMARY KEY (UNITID) )")
    f.close()

def apply_xform():
    """
    Appends the extra columns from the transform step to the schema.
    """
    pass


if __name__ == "__main__":
    for data_path in data_paths:
        extract_csv(data_path)

