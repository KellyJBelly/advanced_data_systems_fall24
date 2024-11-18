import sqlite3
import pandas

## test edge.db
# connect = sqlite3.connect("edge.db")
# cursor = connect.cursor()

# result = cursor.execute("SELECT StartDate,\"Value (kWh)\" FROM PowerUsage_2016_to_2020")
# #could use * as select all in place of field names

# print (result.fetchone())

#function to create new DB with same fields as dataset
def createTSDB(filename):
    connect = sqlite3.connect(filename)
    cursor = connect.cursor()

    #string literal
    result = cursor.execute("""
    CREATE TABLE "PowerUsage_2016_to_2020" (
        "StartDate"	TEXT NOT NULL COLLATE NOCASE,
        "Value (kWh)"	REAL NOT NULL,
        "day_of_week"	INTEGER NOT NULL,
        "notes"	TEXT NOT NULL COLLATE NOCASE
    );
    """)
# createTSDB('cloud.db')

# connect to both edge & cloud tsdbs
edge_connect = sqlite3.connect('edge.db') 
cloud_connect = sqlite3.connect('cloud.db')
edge_curosn = edge_connect.cursor()
cloud_cursor = cloud_connect.cursor()

# partition all but 12,000 rows of data to cloud from edge
# think of enough storage on the device to store the data for an extended network outage
edge_query_szlmt = """
    SELECT StartDate FROM PowerUsage_2016_to_2020 ORDER BY StartDate LIMIT 2000)
    ORDER BY StartDate ASC Limit 1; 
"""

edge_cursor.execute(edge_query_szlmt)