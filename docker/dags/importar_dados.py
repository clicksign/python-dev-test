import os
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from scripts import carga

DATA_PATH =  f"{os.environ['DATA_FILES']}"


tz = pendulum.timezone('America/Sao_Paulo')

args = {
    'owner': 'Rodrigo Brasil',
    'start_date': datetime(2022, 9, 5, tzinfo=tz),
    'depends_on_past': False,
}

dag = DAG(
    'ImportarArquivos',
    default_args=args,
    schedule_interval='0/1 * * * *',
    catchup=False,
)

inicio = DummyOperator(
    task_id='inicio',
    dag=dag,
)

# Task que realiza a leitura dos arquivos de entrada e gera um csv
ler_arquivos = PythonOperator(
    task_id='ler_arquivos',
    dag=dag,
    python_callable=carga.ler_arquivo,
    op_args={
        DATA_PATH
    },
    provide_context=True,
)

# Task que le o arquivo csv ee grava no banco de dados Postgres
gravar_dados = PythonOperator(
    task_id='gravar_dados',
    dag=dag,
    python_callable=carga.gravar_dados,
    provide_context=True,
)

# Remove o arquivo csv e renomeia o arquivo de entrada
remover_arquivo = PythonOperator(
    task_id='remover_arquivo',
    dag=dag,
    python_callable=carga.remover_arquivo,
    provide_context=True,
)

inicio >> ler_arquivos >> gravar_dados >> remover_arquivo