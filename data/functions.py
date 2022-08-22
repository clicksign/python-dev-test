import pandas as pd
import re
from datetime import datetime as dt
import os
import requests

from send_counter import *


def read_description_file(description_file):
    '''
    Read a file and return a list with lines.
    Param
    ------------
        file:   path to description file
    '''
    with open(description_file, 'r') as f:
        text = f.read().splitlines()
    return text


def normalizer(line):
    '''
    Normalizes text to column names
    '''
    return line.replace(':', '').replace('-', '_').strip()


def generate_column_name(text):
    '''
    Read the description file to create a list with column names.
    Param
    ------------
        text:   return from read_description_file function.
    '''

    columns = []
    for sentences in text:
        lines = map(normalizer, re.findall('^\s*\D[^:]+:\s*', sentences))
        values = re.findall(':\s(.*)', sentences)
        for line in lines:
            if line not in columns:
                columns.append(line)
            else:
                continue
    for i in range(2):
        columns.pop()
    return columns


def generate_column_type(text):
    '''
    Read the description file to create a dict with column types.
    Param
    ------------
        text:   return from read_description_file function.
    '''
    columns = []
    dict_type = {}
    for sentences in text:
        lines = map(normalizer, re.findall('^\s*\D[^:]+:\s*', sentences))
        values = re.findall(':\s(.*)', sentences)
        for line in lines:
            if line not in columns:
                columns.append(line)
            else:
                continue
            for value in values:
                if re.search(r'\bcontinuous\b', value):
                    dict_type[line] = 'int'
                else:
                    dict_type[line] = 'str'

    for i in range(2):
        columns.pop()

    for i in range(2):
        dict_type.popitem()
    return dict_type


def get_data(file_path, steps, names, dtypes):
    """
    Returns a subset of rows from a file. The fist [steps]*[count] 
    rows are skipped and the next [steps] rows are returned. 

    params
    ------------
        steps:   number of rows returned
        counter: count variable updated each iteration 
        names:   columns names of dataset
        pathS:    location of csv
    """
    int_columns = []
    str_columns = []
    for key, value in dtypes.items():
        if value == 'int':
            int_columns.append(key)
        else:
            str_columns.append(key)

    request = requests.request(
        "GET", "http://localhost:8000/api/v1/census-etl/counter")
    counter = 0
    data = request.json()
    if len(data) == 0:
        counter = 0
    else:
        for x in request.json():
            counter = x.get('counter')
    if counter == 0:
        try:
            df = pd.read_csv(file_path, nrows=steps,
                             names=names, skipinitialspace=True)
            df = df.astype('str')
            df['counter'] = 1
            df['is_correct'] = True
            for column in int_columns:
                df.loc[df[column].str.contains(
                    '[a-z]', regex=True, case=False) == True, 'is_correct'] = False
            for column in str_columns:
                df.loc[df[column].str.contains(
                    '^([\s\d]+)$', regex=True) == True, 'is_correct'] = False
            df.rename(columns={'class': 'class_category'}, inplace=True)
            return df

        except Exception as e:
            print(e)
    else:
        try:
            df = pd.read_csv(file_path, skiprows=steps*counter,
                             nrows=steps, names=names, skipinitialspace=True)
            df = df.astype('str')
            df['counter'] = counter + 1
            df['is_correct'] = True
            for column in int_columns:
                df.loc[df[column].str.contains(
                    '[a-z]', regex=True, case=False) == True, 'is_correct'] = False
            for column in str_columns:
                df.loc[df[column].str.contains(
                    '^([\s\d]+)$', regex=True) == True, 'is_correct'] = False
            df.rename(columns={'class': 'class_category'}, inplace=True)
            return df

        except Exception as e:
            print(e)


def checkpoint_batch(df, steps):
    """
    Creates a file of checkpoint to garantee the next batch.
    params
    ------------
        df:   Dataframe
    """
    request = requests.request(
        "GET", "http://localhost:8001/api/v1/census-etl/counter")
    if len(request.json()) == 0:
        data = df
        data['counter'] = 1
    else:
        data = df
        for x in request.json():
            freq_value = x.get('counter')
        freq_value += 1
        data['counter'] = freq_value

    for index, row in data.iterrows():
        send_counter(data.to_dict(orient='records'))
        if len(data) != steps:
            frq = data['counter'].max()
            print('Data load finished.')
            print(f'Total of ingestion: {frq*steps}')
            break
        else:
            frq = data['counter'].max()
            send_counter(data.to_dict(orient='records'))
            print(f'Loading data...')
            print(f'Number of ingestion: {frq}')
            print(f'Total of ingestion: {frq*steps}')
            break
