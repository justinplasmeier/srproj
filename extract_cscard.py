import csv
import os
import re

DATA_PATH = '/home/jgp/srproj/data'
data_files = os.listdir(DATA_PATH)
data_paths = [os.path.join(DATA_PATH, x) for x in data_files]

def guess_data_type(value):
   pattern = re.compile("^\-*\d*\.?\d*$")
   to_compare = unicode(value)
   if to_compare == unicode("PrivacySuppressed") or to_compare == unicode("NULL"):
   	return "DECIMAL"
   elif re.match(pattern, to_compare):
   	return "DECIMAL"
   return "VARCHAR(100)"

def extract_csv(filename):

    with open(filename, 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        column_name = list()
        data_type = list()
        i = 0
        latitude = None
        longitude = None
        for val in spamreader.next():
            column_name.append(val)
            # architecture built to be able to modify this stage as needed.
            # This is where we would add other features such as unique constraints, ect.
            if column_name[i] == "ZIP":
                data_type.append(unicode("VARCHAR(10)"))
            else:
      			data_type.append(guess_data_type(val))
            if column_name[i] == "LATITUDE":
                latitude = val
            if column_name[i] == "LONGITUDE":
                longitude = val    
       
            if latitude is not None and longitude is not None:
                data_type.append(unicode("geography(POINT,4326)"))
                column_name.append("COORDINATE")
                latitude = None
                longitude = None
            i+=1

    f = open('schema_cscard.sql', 'w')
    f.truncate()
    f.write("CREATE TABLE college_scorecard ( \n")
    for i in xrange(len(column_name)):
        feature = "  %s %s,\n"%(column_name[i],data_type[i])
        f.write(feature)
    f.write("CONSTRAINT persons_pkey PRIMARY KEY (UNITID) )")
    f.close()


if __name__ == "__main__":
    for data_path in data_paths:
        extract_csv(data_path)

