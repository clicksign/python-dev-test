from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import os
import sqlite3
import sqlalchemy

path = "C:\Users\aless\Downloads\PROJETO CLICKSIGN\dados" 
arq_ini=path + "adult_csv"
ind_ini=0
first_time = True 
data_frame=pd.read_csv(arq_ini)


engine=sqlalchemy.create_engine('sqlite:///adults.db')

def carrega_arquivos(quant):

    if indi_ini > data_frame.shape[0] or os.path.isfile(arq_ini)==False:
        os.system("export AIRFLOW_HOME=$(pwd)/airflow")
        os.system("airflow dags pause ")
        print("end operation")
    else:
        if first_time:
            df = data_frame.iloc[:quant]
            first_time=False
            indi_ini=quant
            df.to_sql('adults',engine,index=True,if_exists='append')
        else:
            df=data_frame.iloc[indi_ini:indi_ini+quant]
            indi_ini=indi_ini + quant
            df.to_sql('adults',engine,index=True,if_exists='append')
default_argument = {
    'owner': 'alessandro gums',
    "email": ['alessandrogums89@gmail.com'],
    'retries': 1,
    }

with DAG(
    
    'carrega dados para bd',
    default_args=default_argument,
    start_date = datetime.now(),
    schedule_interval = timedelta(seconds=10),
    catchup=False,
    
    ) as f:
        task= PythonOperator(
        task_id='carrega arquivos para o banco de dados sqlite',
        python_callable=carrega_arquivos,
        op_kwargs={'quant':1630})
        
      

        