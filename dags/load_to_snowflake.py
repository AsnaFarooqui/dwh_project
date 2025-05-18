from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
import pandas as pd
import pymysql
from datetime import datetime, timedelta
from airflow.sensors.sql import SqlSensor
from airflow.sensors.base import BaseSensorOperator
from pymongo import MongoClient
from airflow.utils.decorators import apply_defaults
import pymongo
import os
from snowflake.connector import connect
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from bson.objectid import ObjectId


base_dir = os.path.dirname(__file__)  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='load_to_snowflake',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='*/4 * * * *',
    catchup=False,
    tags=['etl', 'snowflake'],
)

def get_mysql_conn():
    # Using pymysql or Airflow's MySQL hook
    conn = pymysql.connect(host='172.17.48.1', user='root', password='YourStrongPassword', db='dwh_project')
    return conn

# --- Step 1: Extract MySQL ---
def extract_mysql():
    conn = get_mysql_conn()
    df = pd.read_sql("SELECT * FROM rides", conn)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/rides.csv")
    csv_path = os.path.abspath(csv_path) 
    df.to_csv(csv_path, index=False)
    
    df = pd.read_sql("SELECT * FROM stations", conn)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/stations.csv")
    csv_path = os.path.abspath(csv_path) 
    df.to_csv(csv_path, index=False)
    
    df = pd.read_sql("SELECT * FROM ticket_office", conn)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/ticket_office.csv")
    csv_path = os.path.abspath(csv_path) 
    df.to_csv(csv_path, index=False)

    df = pd.read_sql("SELECT * FROM tickets", conn)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/tickets.csv")
    csv_path = os.path.abspath(csv_path) 
    df.to_csv(csv_path, index=False)
   
    df = pd.read_sql("SELECT * FROM vehicle", conn)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/vehicle.csv")
    csv_path = os.path.abspath(csv_path) 
    df.to_csv(csv_path, index=False)
    
    conn.close()

# --- Step 2: Extract MongoDB ---
def extract_mongo():

    client = pymongo.MongoClient('mongodb://172.17.51.114:27017')
    
    db = client['dwh_project']
    print("MongoDB connection successful:", db.list_collection_names())
    collection = db['commuters']

    data = list(collection.find({},  {"_id": 0}))
    df = pd.DataFrame(data)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/commuter.csv")
    csv_path = os.path.abspath(csv_path)
    df.to_csv(csv_path, index=False)
    print(f"Exported {len(df)} records from commuters collection.")
    collection = db['routes']
    data = list(collection.find({},  {"_id": 0}))
    df = pd.DataFrame(data)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/route.csv")
    csv_path = os.path.abspath(csv_path)
    df.to_csv(csv_path, index=False)

# --- Step 3: Extract Excel ---
def extract_excel():
    excel_path = os.path.join(base_dir, "../project/data/excels/date_data.xlsx")
    excel_path = os.path.abspath(excel_path)  # resolves .. properly
    df = pd.read_excel(excel_path)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/date.csv")
    csv_path = os.path.abspath(csv_path)
    df.to_csv(csv_path, index=False)

    excel_path = os.path.join(base_dir, "../project/data/excels/timeslot_data.xlsx")
    excel_path = os.path.abspath(excel_path)  # resolves .. properly
    df = pd.read_excel(excel_path)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/timeslot.csv")
    csv_path = os.path.abspath(csv_path)
    df.to_csv(csv_path, index=False)

    excel_path = os.path.join(base_dir, "../project/data/excels/events_data.xlsx")
    excel_path = os.path.abspath(excel_path)  # resolves .. properly
    df = pd.read_excel(excel_path)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/events.csv")
    csv_path = os.path.abspath(csv_path)
    df.to_csv(csv_path, index=False)

    excel_path = os.path.join(base_dir, "../project/data/excels/maintenance_data.xlsx")
    excel_path = os.path.abspath(excel_path)  # resolves .. properly
    df = pd.read_excel(excel_path)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/maintenance.csv")
    csv_path = os.path.abspath(csv_path)
    df.to_csv(csv_path, index=False)

    excel_path = os.path.join(base_dir, "../project/data/excels/weather_data.xlsx")
    excel_path = os.path.abspath(excel_path)  # resolves .. properly
    df = pd.read_excel(excel_path)
    csv_path = os.path.join(base_dir, "../project/data/output_csv/weather.csv")
    csv_path = os.path.abspath(csv_path)
    df.to_csv(csv_path, index=False)


# --- Step 4: Upload to Snowflake Stage ---
def upload_to_stage():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/rides.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/route.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/stations.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/weather.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/maintenance.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/ticket_office.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/events.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/commuter.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/date.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/timeslot.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/vehicle.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    file_path = os.path.abspath(os.path.join(base_dir, "../project/data/output_csv/tickets.csv"))
    put_command = f"""
    PUT file://{file_path} @LOAD_DATA AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
    """

    hook.run(put_command)

    
# --- Step 5: Load into Snowflake tables ---
'''copy_data = SnowflakeOperator(
    task_id='copy_into_snowflake',
    snowflake_conn_id='snowflake_conn',
    sql="""
        COPY INTO  FROM @load_data/mysql/mysql_data.csv FILE_FORMAT=(type='CSV' skip_header=1);
        COPY INTO mongo_table FROM @load_data/mongo/mongo_data.csv FILE_FORMAT=(type='CSV' skip_header=1);
        COPY INTO excel_table FROM @load_data/excel/excel_data.csv FILE_FORMAT=(type='CSV' skip_header=1);
    """,
    dag=dag,
)'''

# DAG Tasks
t1 = PythonOperator(task_id='extract_mysql_data', python_callable=extract_mysql, dag=dag)
t2 = PythonOperator(task_id='extract_mongo_data', python_callable=extract_mongo, dag=dag)
t3 = PythonOperator(task_id='extract_excel_data', python_callable=extract_excel, dag=dag)
t4 = PythonOperator(task_id='upload_to_snowflake_stage', python_callable=upload_to_stage, dag=dag)

[t1, t2, t3] >> t4


