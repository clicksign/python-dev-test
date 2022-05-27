from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import os
import sqlite3
import sqlalchemy

path = "D:\GitHub\python-dev-test\data" 
arq_ini=path + "AdultData.csv"
ind_ini=0
first_time = True 
df_AdultData=pd.read_csv(arq_ini)


engine=sqlalchemy.create_engine('sqlite:///AdultData.db')

def carregar_df(quant):

    if indi_ini > df_AdultData.shape[0] or os.path.isfile(arq_ini)==False:
        os.system("export AIRFLOW_HOME=$(pwd)/airflow")
        os.system("airflow dags pause ")
        print("end operation")
    else:
        if first_time:
            df = df_AdultData.iloc[:quant]
            first_time=False
            indi_ini=quant
            df.to_sql('AdultData',engine,index=True,if_exists='append')
        else:
            df=df_AdultData.iloc[indi_ini:indi_ini+quant]
            indi_ini=indi_ini + quant
            df.to_sql('AdultData',engine,index=True,if_exists='append')
default_argument = {
    'owner': 'Joao Rafael Quadros',
    "email": ['j.rafael@live.com'],
    'retries': 1,
    }

with DAG(
    
    'carrega dados para o banco de dados',
    default_args=default_argument,
    start_date = datetime.now(),
    schedule_interval = timedelta(seconds=10),
    catchup=False,
    
    ) as f:
        task= PythonOperator(
        task_id='carrega arquivos para o banco de dados sqlite',
        python_callable=carregar_df,
        op_kwargs={'quant':1630})