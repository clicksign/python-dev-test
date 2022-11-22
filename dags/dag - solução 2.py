# --------------------------------------------------------------
# DAG that orchestrates the execution of the Python script that
# insert the US census bureau data into the Postgres databse
# --------------------------------------------------------------


import io
import time
import json
import boto3
import psycopg2
import pandas as pd
import pyarrow as pa
from airflow import DAG
from typing import List
from io import StringIO
import pyarrow.parquet as pq
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


# Contrainsts

file = open('/opt/airflow/dags/constraints.json')
constraints = json.load(file)
file.close()

# S3 Client and Resource

s3_client = boto3.client(
    's3',
    aws_access_key_id=constraints['aws_access_key_id'],
    aws_secret_access_key=constraints['aws_secret_access_key']
)

s3_resource = boto3.resource(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id=constraints['aws_access_key_id'],
    aws_secret_access_key=constraints['aws_secret_access_key']
)

# Functions

def read_raw_data(s3_client, bucket: str, raw_path: str, refined_path: str, **kwargs):
    """
    This function reads the raw datasets from S3 bucket, 
    apply the needed transformations and adjustments,
    and write the output dataset to S3 refined folder
    """

    # Read the raw datasets from S3 raw folder

    datasets = []

    try:
        result = s3_client.list_objects(Bucket=bucket, Prefix=raw_path)
        contents = result.get('Contents')
        for obj in contents:
            content = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
            data = content['Body'].read().decode()
            datasets.append(data)

    except Exception as e:
        print(e)

    # Apply the needed transformations adjustments

    i = 0

    columns = ['age', 'workclass', 'fnlwgt',
              'education', 'education-num', 'marital-status',
              'occupation', 'relationship', 'race',
              'sex', 'capital-gain', 'capital-loss',
              'hours-per-week', 'native-country', 'class']

    for dataset in datasets:
                
        i = i + 1

        df = pd.read_csv(StringIO(dataset), sep=',', names=columns)
        df = df.infer_objects()
        df = df.dropna(how='any')
    
        # Adjust the numeric columns

        num_columns = [
            'age', 'fnlwgt', 'education-num',
            'capital-gain', 'capital-loss', 'hours-per-week'
        ]

        for column in num_columns:
            if df[f'{column}'].dtypes == 'float64' or df[f'{column}'].dtypes == 'int64':
                pass
            else:
                df[f'{column}_check'] = list(map(lambda x: x.isalpha(), df[f'{column}'].str.strip()))
                df.loc[df[f'{column}_check'] == True, f'{column}'] = 0
                df.drop(f'{column}_check', axis=1, inplace=True)

        # Adjust the datatypes

        df = df.astype(
            {
                "age": int,
                "fnlwgt": int,
                "education-num": int,
                "capital-gain": int,
                "capital-loss": int,
                "hours-per-week": int
            }
        )

        # Save the dataframe to S3 refined folder

        parquet_table = pa.Table.from_pandas(df)
        pq.write_table(parquet_table, 'adult')

        s3_client.upload_file('adult', bucket, f'{refined_path}adult{i}.parquet')

def insert_into_database(s3_client, s3_resource, bucket: str, refined_path: str, **kwargs):
    """
    This function reads the parquet files from S3 bucket refined folder, 
    and insert the data into a Postgres database table
    """

    # Postgres database connection and cursor
    conn = psycopg2.connect(
        "dbname='{db}' user='{user}' host='{host}' port='{port}' password='{passwd}'"
        .format(
            user='postgres',
            passwd='Gf2023!@',
            host='host.docker.internal',
            port='5432',
            db='postgres'
        )
    )

    cur = conn.cursor()

    # Read the parquet files from the S3 refined folder

    result = s3_client.list_objects(Bucket=bucket, Prefix=refined_path)
    contents = result.get('Contents')

    dfs = []

    for obj in contents:
        obj_size = obj['Size']
        if obj_size == 0:
            pass
        else:
            buffer = io.BytesIO()
            object = s3_resource.Object(bucket, obj['Key'])
            object.download_fileobj(buffer)
            df = pd.read_parquet(buffer)
            dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    # Split the dataframe into chunks

    chunks = list()
    chunk_size = 1630
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])

    # Insert the chunks into Postgres table

    truncate_stmt = "TRUNCATE TABLE public.adult"
    cur.execute(truncate_stmt)

    counter = 0

    for chunk in chunks:

        counter += 1

        try:
            output = io.StringIO()
            chunk.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            contents = output.getvalue()
            cur.copy_from(output, 'adult', null="")
            conn.commit()
            print(f"Chunk {counter} inserted into Postgres table")
            print("The next chunk will be inserted after 10 seconds")
            time.sleep(10)

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            return 1

DAG

default_args = {
    'owner': 'Gustavo Franco',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['gsvfranco@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'desafio_dev_pyton_solucao_2',
    default_args=default_args,
    description='Desafio - Dev Python - Solução 2',
    schedule=timedelta(minutes=6),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='read_raw_data',
        python_callable=read_raw_data,
        op_kwargs={
            's3_client': s3_client,
            'bucket': constraints['bucket'], 
            'raw_path': constraints['raw_path'],
            'refined_path': constraints['refined_path']
        }
    )

    t2 = PythonOperator(
        task_id='insert_into_database',
        python_callable=insert_into_database,
        op_kwargs={
            's3_client': s3_client,
            's3_resource': s3_resource,
            'bucket': constraints['bucket'], 
            'refined_path': constraints['refined_path']
        }
    )

    t1 >> t2
