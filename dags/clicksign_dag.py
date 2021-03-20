from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

from LimpezaDados import limpeza
from ExportaDados import exporta_dados
from query import query

default_args = {
            "owner": "airflow",
            "start_date": datetime(2021, 3, 16),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": "edson.costa@hotmail.com",
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        }

with DAG(dag_id="clicksign_dag_edson", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    t1 = PythonOperator(task_id='limpeza_dados', python_callable=limpeza)

    t2 = BashOperator(task_id='checagem_csv', bash_command='shasum ~/dags/clicksign_df.csv', retries=2, retry_delay=timedelta(seconds=15))

    t3 = PythonOperator(task_id='criar_banco', python_callable=exporta_dados)

    t4 = PythonOperator(task_id='query', python_callable=query)


t1 >> t2 >> t3 >> t4