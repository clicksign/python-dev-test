from utils.get_data import get_data as gd
from operators.data_to_postgres.data import DataToPostgresOperator
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import pandas as pd
import datetime, re
import logging

path_file = "inputs/data/Adult.data.txt"
path_query = "inputs/queries/create/adult.sql"
conn_id = "adult_db_id"

cols_type = {          
    "age": "int"
    ,"workclass":"category"
    ,"fnlwgt":"int"
    ,"education":"category"
    ,"education-num":"int"
    ,"marital-status":"category"
    ,"occupation":"category"
    ,"relationship":"category"
    ,"race":"category"
    ,"sex":"category" 
    ,"capital-gain":"float"      
    ,"capital-loss":"float"      
    ,"hours-per-week":"int"    
    ,"native-country":"category"
    ,"class":"category"  
}

# Declated DAG with parameters
dag = DAG(
    dag_id="data_to_postgres",
    schedule_interval = "0 */1 * * *",
    dagrun_timeout=datetime.timedelta(minutes=60),
    start_date = days_ago(0)
) 

# Creating task to create table
task_create_table = DataToPostgresOperator(
                    task_id = "task_create_table",
                    conn_id = "adult_db_id",
                    path_file = path_query,
                    method = "execute",
                    execution_timeout=datetime.timedelta(hours=2),
                    dag = dag
                )
# Creating a task to truncate table
task_truncate_table = DataToPostgresOperator(
                    task_id = "task_truncate_table",
                    conn_id = "adult_db_id",
                    method = "truncate",
                    table_name = "adult",
                    execution_timeout=datetime.timedelta(hours=2),
                    dag = dag
                )
# Create task to oinsert table
task_insert_data = DataToPostgresOperator(
                   task_id = "task_insert_data",
                   method = "insert_df_pandas",
                   table_name = "adult",
                   path_file = path_file,
                   cols_type = cols_type,
                   range_data = 1630,
                   conn_id = conn_id,
                   execution_timeout=datetime.timedelta(hours=2),
                   step_time = 0,
                   dag = dag   
                )

task_create_table >> task_truncate_table >> task_insert_data