import pandas
import pyodbc
import csv

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=204.141.52.148;DATABASE=DBKiero_Productos;UID=MachineBaseConnect3651;PWD=H1#KotS(xh5nF+tGv')
cursor = conn.cursor()
query = 'select top(10) * from tbl_Productos'

cursor.execute(query)
with open("out.csv","a") as outfile:
    writer = csv.writer(outfile, delimiter = '|', quoting=csv.QUOTE_NONNUMERIC)

    writer.writerow(col[0] for col in cursor.description)
    for row in cursor:
        writer.writerow(row)

