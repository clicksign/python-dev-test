from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from census_bureau_etl import (
    extract_census_bureau,
    transform_census_bureau,
    load_census_bureau,
)


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
}

dag = DAG(
    "etl_census_bureau",
    default_args=DEFAULT_ARGS,
    description="ETL for Census Bureau",
    schedule_interval=timedelta(seconds=10),
)

extract_census_bureau_task = PythonOperator(
    task_id="extract_census_bureau",
    python_callable=extract_census_bureau,
    dag=dag,
)

transform_census_bureau_task = PythonOperator(
    task_id="transform_census_bureau",
    python_callable=transform_census_bureau,
    dag=dag,
)

load_census_bureau_task = PythonOperator(
    task_id="load_census_bureau",
    python_callable=load_census_bureau,
    dag=dag,
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv",
    dag=dag,
)

(
    extract_census_bureau_task
    >> transform_census_bureau_task
    >> load_census_bureau_task
    >> clean_task
)
