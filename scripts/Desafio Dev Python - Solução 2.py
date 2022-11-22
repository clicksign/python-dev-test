# -----------------------------------------------------------
# Script that transports data from some datasets and to Postgresql table
#
# Gustavo Franco
# email gsvfranco@gmail.com
# -----------------------------------------------------------


import os
import time
import psycopg2
import pandas as pd
from io import StringIO
from typing import List


def read_datasets(path: str, filenames: List[str]):
    """
    This function reads the datasets with
    US Census Bureau data from local file system

    :param path: Path where the datasets files are in local file system
    :param filenames: Names of the files
    :return: A list with the datasets appended as strings
    """
    try:
        with open('output_file', 'w') as outfile:
            for filename in filenames:
                with open(os.path.join(path, filename)) as infile:
                    contents = infile.read()
                    outfile.write(contents)
            with open('output_file', 'r') as file:
                dataset = file.read()
                file.close()

        os.remove('C:/Users/LowCost/PycharmProjects/TOTVS/output_file')

        return dataset

    except Exception as e:
        print(e)

def create_dataframe(dataset: List[str], columns: List[str]):
    """
    This function creates a pandas dataframe
    with the datasets read from local file system

    :param dataset: List of the datasets read from local file system
    :param columns: List of the dataframe columns
    :return: A pandas dataframe of the appended datasets
    """
    df = pd.read_csv(StringIO(dataset), sep=',', names=columns)
    df = df.infer_objects()
    df = df.dropna(how='any')
    return df

def split_dataframe(df, chunk_size: int):
    """
    This function splits the pandas dataframe into chunks of 1630 size

    :param df: The dataframe to split
    :param chunk_size: The size of the chunks
    :return: A list with the chunks splitted
    """
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

def adjust_dataframe(df):
    """
    This function checks if the numeric columns of the
    dataframe contain alphanumeric characters, cleaning
    and adjusting these inconsistent values, to allow
    it to be stored as integers in the database

    :param df: The dataframe to check/adjust
    :return: The dataframe adjusted
    """
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

    return df

def adjust_dtypes(df):
    """
    This function adjusts the numeric columns
    datatypes, to be correctly stored in the database

    :param df: The dataframe to check/adjust
    :return: The dataframe with the datatypes of the numeric columns adjusted
    """
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
    return df

def insert_into_table(conn, cur, chunks, table: str):
    """
    This function writes the dataframe cleaned and
    prepared, into a Postgres database table

    :param df: The dataframe to be written
    :param df: The name of the table
    """
    counter = 0
    truncate_stmt = "TRUNCATE TABLE {}".format(table)
    cur.execute(truncate_stmt)
    conn.commit()
    for chunk in chunks:
        counter += 1
        tuples = list(set([tuple(x) for x in chunk.to_numpy()]))
        query = "INSERT INTO {} VALUES (" \
                "%s, %s, %s, %s, %s, %s, %s, %s, " \
                "%s, %s, %s, %s, %s, %s, %s)".format(table)
        try:
            cur.executemany(query, tuples)
            conn.commit()
            print(f"Chunk {counter} inserted into Postgres table")
            print("The next chunk will be inserted after 10 seconds")
            time.sleep(10)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            return 1

def main():

    # Constraints

    path = 'C:/Users/LowCost/Documents/Desafio - Dev Python'
    filenames = ['Adult.data', 'Adult.test']

    df_columns = ['age', 'workclass', 'fnlwgt',
                  'education', 'education-num', 'marital-status',
                  'occupation', 'relationship', 'race',
                  'sex', 'capital-gain', 'capital-loss',
                  'hours-per-week', 'native-country', 'class']

    print("1. Creating Postgres connection")

    conn = psycopg2.connect(
        "dbname='{db}' user='{user}' host='{host}' port='{port}' password='{passwd}'"
        .format(
            user='postgres',
            passwd='Gf2023!@',
            host='localhost',
            port='5432',
            db='postgres'
        )
    )

    print(conn)
    print()

    print("2. Creating Postgres cursor")

    cur = conn.cursor()

    print(cur)
    print()

    print("3. Reading datasets from file system")

    datasets = read_datasets(path, filenames)

    print("Files: " + str(filenames))
    print()

    print("4. Creating pandas dataframe")

    df = create_dataframe(datasets, columns=df_columns)

    print("Dataframe rows count: " + str(len(df)))
    print()

    print("5. Cleaning and adjusting dataframe")

    df = adjust_dataframe(df)
    df = adjust_dtypes(df)

    print()

    print("6. Splitting pandas dataframe into chunks")

    chunk_size = 1630
    chunks = split_dataframe(df, chunk_size)

    print(str(len(chunks)) + f" chunks of size {chunk_size} created")
    print()

    print("6. Inserting chunks into Postgres database table")

    table = 'adult'
    insert_into_table(conn, cur, chunks, table)

if __name__ == '__main__':
    main()

