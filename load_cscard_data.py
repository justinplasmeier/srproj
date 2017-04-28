import psycopg2
import sys

SQL_STATEMENT = """
    COPY %s FROM STDIN WITH
        CSV
        DELIMITER AS ','
    """
my_file = open("/home/jgp/geocivics/load/data/college_scorecard.csv")

def process_file(conn, table_name, file_object):
    cursor = conn.cursor()
    cursor.copy_expert(sql=SQL_STATEMENT % table_name, file=file_object)
    conn.commit()
    cursor.close()

#Define our connection string
conn_string = "host='localhost' dbname='postgres' user='zij'"

COUNT = 0
with open('/home/jgp/geocivics/srproj/count.txt') as f:
    COUNT = int(f.read())
COUNT-=1
table_name = "college_scorecard_"+str(COUNT)
connection = psycopg2.connect(conn_string)
try:
    process_file(connection, table_name, my_file)
finally:
    connection.close()
