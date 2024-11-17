import sqlite3


connect = sqlite3.connect("edge.db")
cursor = connect.cursor()

result = cursor.execute("SELECT StartDate,\"Value (kWh)\" FROM PowerUsage_2016_to_2020")
#could use * as select all in place of field names

print (result.fetchone())