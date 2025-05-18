import pandas as pd
import pymysql
from pymongo import MongoClient
from snowflake_utils import get_snowflake_connection
import os

# CSV or Excel to Snowflake
def upload_dataframe_to_snowflake(df, table_name):
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    # Create INSERT statement (assuming tables already exist)
    columns = ','.join(df.columns)
    values = ','.join(['%s'] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

    # Convert df to list of tuples
    data = [tuple(x) for x in df.values]

    # Execute many
    cursor.executemany(insert_sql, data)
    conn.commit()

def upload_excels():
    for filename in os.listdir('./data/excels'):
	if filename.endswith('.xlsx'):
	    table = filename.replace('data.xlsx', 'raw')
	    df = pd.read_excel(f'./data/excels/{filename}')
	    upload_dataframe_to_snowflake(df, table)

# MySQL to Snowflake
def upload_mysql_table(mysql_table, snowflake_table):
    mysql_conn = pymysql.connect(
	host='localhost',
	user='root',
	password='Aassnnaa',
	database='dwh_project'
    )
    df = pd.read_sql(f"SELECT * FROM {mysql_table}", mysql_conn)
    upload_dataframe_to_snowflake(df, snowflake_table)

# MongoDB to Snowflake
def upload_mongo_collection(mongo_collection, snowflake_table):
    client = MongoClient('mongodb://localhost:27017')
    db = client['dwh_project']
    collection = db[mongo_collection]

    data = list(collection.find({}, {'_id': 0})) # To exclude MongoDB _id
    df = pd.DataFrame(data)
    upload_dataframe_to_snowflake(df, snowflake_table)

# Main Upload Pipeline
def main():
    upload_excels()
    upload_mysql_table("rides", "RIDES_RAW")
    upload_mysql_table("stations", "STATIONS_RAW")
    upload_mysql_table("ticket_office", "TICKET_OFFICE_RAW")
    upload_mysql_table("tickets", "TICKETS_RAW")
    upload_mysql_table("vehicle", "VEHICLE_RAW")
    upload_mongo_collection("commuters", "COMMUTER_RAW")
    upload_mongo_collection("routes", "ROUTES_RAW")
