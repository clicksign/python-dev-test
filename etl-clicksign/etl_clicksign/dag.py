import airflow


from models import Adult
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, SqliteOperator


dag = DAG(
    'insert_data_adult', 
    schedule_interval='*/30 * * * * *'
)


def count_records_adult():
    "Ler quantos records ja existem na tabela"
    return Adult.select().count()

def count_records_data_adult():
    "Ler arquivo Adult.data e retornar numero de linhas"
    with open(r"data/Adult.data", 'r') as adult_data:
        file_size = len(adult_data.readlines())
    return file_size 

def extract_data():
    #TODO: extrair 1.630 por vez do arquivo Adult.data
    #OBS: Contar as linhas a partir do contador da tabela 
    
    return "List com 1630 linhas"
    

def transform_obj():
    #TODO: transform cada linha em Adult Obj
    return "List com 1630 objetos Adult"

def load_sqlite():
    #TODO: Carregar 1630 objetos Adult no banco
    return "Carregar 1630 objetos Adult no banco"

create_table_sqlite_task = SqliteOperator(
    task_id='create_adult_table_sqlite',
    sql=r"""
    CREATE TABLE IF NOT EXISTS Adult (
        age INT,
        workclass TEXT,
        fnlwgt INT,
        education TEXT,
        education_num INT,
        marital_status TEXT,
        relationship TEXT,
        race TEXT,
        sex TEXT,
        capital_gain INT,
        capital_loss INT,
        hours_per_week INT,
        native_country TEXT,
        _class TEXT
    );
    """,
    dag = dag
)

extract = PythonOperator(task_id='extract_data',
                   provide_context=True,
                   python_callable=extract_data,
                   dag=dag)

transform = PythonOperator(task_id='transform_obj',
                   provide_context=True,
                   python_callable=transform_obj,
                   dag=dag)

load = PythonOperator(task_id='load_sqlite',
                   provide_context=True,
                   python_callable=load_sqlite,
                   dag=dag)


create_table_sqlite_task >> extract >> transform >> load

