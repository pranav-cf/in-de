import psycopg2
import pandas as pd

#DB details
dest_conn = psycopg2.connect(
    host="localhost",
    database="sentree",
    user="sentree",
    password="password")

dest_cur = dest_conn.cursor()

#constants
filename = "in_global.dsv"
insert_query = """insert into @ (#) values ($)"""
chunksize = 3
table = "table_global"


#Get insert query according to column names
dest_cur.execute(f"select * from {table} limit 1")
dest_columns = dest_cur.description

tmp_insert_query = insert_query
for col in dest_columns:
    tmp_insert_query = tmp_insert_query.replace('#', col.name + ',#').replace("$", "{},$")
tmp_insert_query = tmp_insert_query.replace(',#', '').replace(",$", '').replace('@', table)

print(tmp_insert_query)

def process(result):
    typ = type(result)
    # print(typ)
    if result is None:
        return 'null'
    if typ is pd._libs.tslibs.timestamps.Timestamp:
        return "'{}'".format(result.strftime("%Y-%m-%d"))
    if typ is str and "'" in result:
        result = result.replace("'", "''")
    if typ is str:
        return f"'{result}'"
    return result

df = pd.read_csv(filename, chunksize=chunksize, sep="|", header=0, dtype=str)

#read data from file and insert in db
for chunk in df:
    #prepocessing
    chunk['DOB'] = pd.to_datetime(chunk['DOB'], format='%d%m%Y')
    chunk['Open_Date'] = pd.to_datetime(chunk['Open_Date'], format='%Y%m%d')
    chunk['Last_Consulted_Date'] = pd.to_datetime(chunk['Last_Consulted_Date'], format='%Y%m%d')

    #load each row in db
    for idx, row in chunk.iterrows():
        row = [process(x) for x in [row[ "Customer_Name"], row["Customer_Id"], row[ "Open_Date"],
              row[ "Last_Consulted_Date"], row["Vaccination_Id"], row[ "Dr_Name"],
              row[ "State"], row["Country"], row["DOB"], row["Is_Active"]]]
        tmp = tmp_insert_query.format(*row)
        dest_cur.execute(tmp)
        print(tmp)

dest_conn.commit()
dest_conn.close()

