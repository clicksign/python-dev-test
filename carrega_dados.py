from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import os
import sqlite3
import sqlalchemy

# importando arquivos e criando dataframes
path = "D:\GitHub\python-dev-test\data" 
arq_ini1=path + "\AdultData.csv"
arq_ini2=path + "\AdultTest.csv"
indi_ini1=0
indi_ini2=0
first_time = True 
data_frame1=pd.read_csv(arq_ini1)
data_frame2=pd.read_csv(arq_ini2)

engine=sqlalchemy.create_engine('sqlite:///adultDataTest.db')

#função para carregar o DF adultData
def carrega_adultData(x):
    if indi_ini > data_frame1.shape[0] or os.path.isfile(arq_ini1)==False:
        os.system("export AIRFLOW_HOME=$(pwd)/airflow")
        os.system("airflow dags pause ")
        print("encerrar")
    else:
        if first_time:
            df = data_frame1.iloc[:x]
            first_time=False
            indi_ini1=x
            df.to_sql('adults',engine,index=True,if_exists='append')
        else:
            df=data_frame1.iloc[indi_ini:indi_ini+x]
            indi_ini=indi_ini + x
            df.to_sql('adults',engine,index=True,if_exists='append')

default_argument = {
    'owner': 'João Rafael Quadros',
    "email": ['j.rafael@live.com'],
    'retries': 1,
    }

#função para carregar o DF adultTest
def carrega_adultTest(y):
    if indi_ini2 > data_frame2.shape[0] or os.path.isfile(arq_ini2)==False:
        os.system("export AIRFLOW_HOME=$(pwd)/airflow")
        os.system("airflow dags pause ")
        print("encerrar")
    else:
        if first_time:
            df = data_frame2.iloc[:y]
            first_time=False
            indi_ini2=y
            df.to_sql('adults',engine,index=True,if_exists='append')
        else:
            df=data_frame2.iloc[indi_ini2:indi_ini2+y]
            indi_ini2=indi_ini2 + y
            df.to_sql('adults',engine,index=True,if_exists='append')

default_argument = {
    'owner': 'João Rafael Quadros',
    "email": ['j.rafael@live.com'],
    'retries': 1,
    }

#criação da dag com o schedule interval definido para execução de 10 em 10 segundos conforme orientações
with DAG(
    
    'adultDataTest',
    default_args=default_argument,
    start_date = datetime(2022,5,27,13),
    schedule_interval = timedelta(seconds=10),
    catchup=False,
    
    ) as dag:

        taskAdultData= PythonOperator(
        task_id='carrega_adultData',
        python_callable=carrega_adultData,
        op_kwargs={'quant':1630}, # numero de registros a serem processados na task
        dag=dag)

        taskAdultTest =  PythonOperator(
        task_id='carrega_adultTest',
        python_callable=carrega_adultTest,
        op_kwargs={'quant':1630}, # numero de registros a serem processados na task
        dag=dag)

        taskAdultData >> taskAdultTest # criado 2 tasks, definindo ordem de execução


#################################

# agrupando por taskGroup




""" from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import os
import sqlite3
import sqlalchemy

path = "D:\GitHub\python-dev-test\data" 
arq_ini1=path + "\AdultData.csv"
arq_ini2=path + "\AdultTest.csv"
indi_ini1=0
indi_ini2=0
first_time = True 
data_frame1=pd.read_csv(arq_ini1)
data_frame2=pd.read_csv(arq_ini2)

engine=sqlalchemy.create_engine('sqlite:///adultDataTest.db')

@task(op_kwargs={'x':1630})
def carrega_adultData(x):
    if indi_ini > data_frame1.shape[0] or os.path.isfile(arq_ini1)==False:
        os.system("export AIRFLOW_HOME=$(pwd)/airflow")
        os.system("airflow dags pause ")
        print("encerrar")
    else:
        if first_time:
            df = data_frame1.iloc[:x]
            first_time=False
            indi_ini1=x
            df.to_sql('adults',engine,index=True,if_exists='append')
        else:
            df=data_frame1.iloc[indi_ini:indi_ini+x]
            indi_ini=indi_ini + x
            df.to_sql('adults',engine,index=True,if_exists='append')

default_argument = {
    'owner': 'João Rafael Quadros',
    "email": ['j.rafael@live.com'],
    'retries': 1,
    }

@task(op_kwargs={'y':1630})
def carrega_adultTest(y):
    if indi_ini2 > data_frame2.shape[0] or os.path.isfile(arq_ini2)==False:
        os.system("export AIRFLOW_HOME=$(pwd)/airflow")
        os.system("airflow dags pause ")
        print("encerrar")
    else:
        if first_time:
            df = data_frame2.iloc[:y]
            first_time=False
            indi_ini2=y
            df.to_sql('adults',engine,index=True,if_exists='append')
        else:
            df=data_frame2.iloc[indi_ini2:indi_ini2+y]
            indi_ini2=indi_ini2 + y
            df.to_sql('adults',engine,index=True,if_exists='append')

default_argument = {
    'owner': 'João Rafael Quadros',
    "email": ['j.rafael@live.com'],
    'retries': 1,
    }

with DAG(dag_id = 'adultDataTest', schedule_interval = timedelta(seconds=10), start_date = datetime(2022,5,27,13),default_args=default_argument,catchup=False, tags =("adultDataTest") ) as dag:

    with TaskGroup("adultDataTestGroup", tooltip= "Extrair e Carregar Dados") as extrair_carregar:
        carrega_adultData
        carrega_adultTest
        #definindo ordem
        carrega_adultData >> carrega_adultTest """
