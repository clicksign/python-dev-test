import pandas as pd
import re
from datetime import datetime as dt


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
        file_path:    location of data file
    """
    int_columns = []
    str_columns = []
    for key, value in dtypes.items():
        if value == 'int':
            int_columns.append(key)
        else:
            str_columns.append(key)

    if os.path.exists('checkpoint.json') is False:
        counter = 0
    else:
        data = pd.read_json('checkpoint.json')
        counter = data['counter'].max()
    if counter == 0:
        try:
            df = pd.read_csv(file_path, nrows=steps,
                             names=names, skipinitialspace=True)
            df = df.astype('str')
            df['is_correct'] = True
            for column in int_columns:
                df.loc[df[column].str.contains(
                    '[a-z]', regex=True, case=False) == True, 'is_correct'] = False
            for column in str_columns:
                df.loc[df[column].str.contains(
                    '^([\s\d]+)$', regex=True) == True, 'is_correct'] = False
                return df
        except Exception as e:
            print(e)
    else:
        try:
            df = pd.read_csv(file_path, skiprows=steps*counter,
                             nrows=steps, names=names, skipinitialspace=True)
            df = df.astype('str')
            df['is_correct'] = True
            for column in int_columns:
                df.loc[df[column].str.contains(
                    '[a-z]', regex=True, case=False) == True, 'is_correct'] = False
            for column in str_columns:
                df.loc[df[column].str.contains(
                    '^([\s\d]+)$', regex=True) == True, 'is_correct'] = False
            return df
        except Exception as e:
            print(e)

    def get_removed_data(df):
        """
        Creates a file with removed data with some data inconsistency,
        found in the previous step.

        params
        ------------
            df:   Dataframe
        """
        if os.path.exists('removed_data.json') is False:
            removed_data = df.loc[df['is_correct'] == False]
            removed_data['extract_date'] = dt.now().strftime(
                "%Y-%m-%d %H:%M:%S.%f")
            removed_data = removed_data.reset_index()
            removed_data.drop(columns='index', inplace=True)
            return removed_data.to_json(os.getcwd()+'/removed_data.json')
        else:
            for idx, row in df.iterrows():
                removed_data = pd.read_json(os.getcwd()+'/removed_data.json')
                removed_data.drop_duplicates(ignore_index=True, keep='first')
                removed_data = removed_data.append(
                    df.loc[df['is_correct'] == False])
                removed_data = removed_data.reset_index()
                removed_data.drop(columns='index', inplace=True)
                removed_data['extract_date'] = dt.now().strftime(
                    "%Y-%m-%d %H:%M:%S.%f")
                return removed_data.drop_duplicates(ignore_index=True, keep='first').to_json(os.getcwd()+'/removed_data.json')


def checkpoint_batch(df):
    """
    Creates a file of checkpoint to garantee the next batch.
    params
    ------------
        df:   Dataframe
    """
    if os.path.exists('checkpoint.json') is False:
        counter = 0
    else:
        data = pd.read_json('checkpoint.json')
        counter = data['counter'].max()

    for index, row in df.iterrows():
        # Update count
        counter += 1
        df['counter'] = counter
        df['last_index'] = counter*steps
        if len(df) != steps:
            print('Data load finished.')
            print(f'Total of ingestion: {counter*steps}')
            break
        else:
            df.to_json(os.getcwd()+'/checkpoint.json')
            print(f'Loading data...')
            print(f'Number of ingestion: {counter}')
            print(f'Total of ingestion: {counter*steps}')
            break
