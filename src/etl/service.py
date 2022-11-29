from typing import List
import pandas as pd
import os
import numpy as np
from pandas.core.frame import DataFrame

from src.infrastructure.core import Settings
from src.infrastructure.core.models import Adult
from src.infrastructure.database.session import dbconn
from src.infrastructure.core.repository import AdultRepo
from src.etl.constants import ColumnType


base_path = Settings.PATH.value


class EtlService:

    def __init__(self) -> None:
        self._conn = dbconn
        self._repo = AdultRepo
        self._model = Adult

    def select_dataset(self) -> DataFrame:
        engine = self._conn.engine()
        is_db = self._repo.check_data_in_db(engine)
        if is_db:
            dataset = self.fetch_from_buffer()
        else:
            dataset = self.fetch_from_file()
        return dataset

    def fetch_from_file(self) -> DataFrame:
        """
        This function takes the two datasets, join them and resets the index
        """
        print('\nExtraction data from files START...')
        columns = ColumnType.values()
        df_data = pd.read_csv(
            filepath_or_buffer=f'{base_path}/data/Adult.data',
            header=None,
            names=columns
        )
        df_test = pd.read_csv(
            filepath_or_buffer=f'{base_path}/data/Adult.test',
            header=None,
            names=columns,
            skiprows=1
        )
        df = pd.concat([df_data, df_test])
        df = df.reset_index(drop=True)
        sample = df[:1630]
        self._save_buffer(
            dataframe=df[1630:],
            path=f'{base_path}/data/AdultBuffer.data'
        )
        print('Extraction data from files END!!!')
        return sample

    def fetch_from_buffer(self) -> DataFrame:
        print('Extraction data from buffer START...')
        columns = ColumnType.values()
        df = pd.read_csv(
            filepath_or_buffer=f'{base_path}/data/AdultBuffer.data',
            header=None,
            names=columns
        )
        sample = df[:1630]
        self._save_buffer(
            dataframe=df[1630:],
            path=f'{base_path}/data/AdultBuffer.data'
        )
        print('Extraction data from buffer END!!!')
        return sample

    def transforms_data(self, data: DataFrame):
        print('Transformation database START...')
        dataset = self._validate_integer_values(
            dataframe=data,
            columns=['age', 'fnlwgt', 'capital-gain', 'hours-per-week']
        )

        dataset = self._validate_string_values(dataframe=dataset)
        print('Transformation database END!!!')
        return dataset
    
    def save_in_db(self, dataframe: DataFrame) -> int:
        print('Loading dataset in db START!!!')
        adults = []
        length = len(dataframe)
        for index in range(length):
            keys = [
                'age', 'workclass', 'fnlwgt', 'education', 'education_num',
                'marital_status', 'occupation', 'relationship', 'race',
                'sex', 'capital_gain', 'capital_loss', 'hours_per_week',
                'native_country', 'class_'
            ]
            values = list(dataframe.values[index])
            data = dict(zip(keys, values))
            adults.append(data)
        self._repo.insert_bulk(self._conn.session(), adults)
        return length

    def _validate_integer_values(self, dataframe: DataFrame, columns: List[str]) -> DataFrame:
        """This function validates columns that should be integer but are string.
        Remove rows that are not of type int and change the column type

        Args:
            df (DataFrame): dataset
            columns (List[str]): lista das colunas
        """

        for column in columns:
            
            for row in dataframe[column]:
                try:
                    int(row)
                except ValueError:
                    index = int(dataframe[dataframe[column] == row].index.values)
                    dataframe = dataframe.drop(index=index)
            
            dataframe[column] = dataframe[column].astype('int')
        
        dataframe = dataframe.reset_index(drop=True)
        return dataframe

    def _validate_string_values(self, dataframe: DataFrame) -> DataFrame:
        """This function validate string type columns

        Args:
            dataframe (DataFrame): dataset
        """
        df = self._sanitize(dataframe=dataframe)
        
        df = self._replace_values(dataframe=df)
        
        df = self._drop_null_registers(dataframe=df, column='native-country')
        
        df = self._drop_null_registers(dataframe=df, column='occupation')
        
        df = df.drop_duplicates()
        
        df = df.reset_index(drop=True)

        return df

    def _sanitize(self, dataframe: DataFrame) -> DataFrame:
        """This function drop the blank spaces in columns by type string

        Args:
            dataframe (DataFrame): dataset

        Returns:
            DataFrame: dataset
        """
        for column in dataframe.columns:
            if dataframe[column].dtypes == 'object':
                dataframe[column] = dataframe[column].str.strip()
        return dataframe

    def _replace_values(self, dataframe: DataFrame) -> DataFrame:
        """This function replace some values the dataset

        Args:
            dataframe (DataFrame): dataset

        Returns:
            DataFrame: dataset
        """
        dataframe.replace('?', np.NaN, inplace=True)
        dataframe['class'] = dataframe['class'].replace('<=50K.', '<=50K')
        dataframe['class'] = dataframe['class'].replace('>50K.', '>50K')
        dataframe['workclass'] = dataframe['workclass'].replace(np.NaN, 'Private')
        return dataframe

    def _drop_null_registers(self, dataframe: DataFrame, column: str) -> DataFrame:
        """This function remove null registers by index dataset

        Args:
            dataframe (DataFrame): dataset
        
        Returns:
            DataFrame: dataset
        """
        ids = dataframe[dataframe[column].isnull()].index.values
        dataframe = dataframe.drop(index=list(ids))
        return dataframe

    def _save_buffer(self, dataframe: DataFrame, path: str) -> None:
        if len(dataframe) <= 1630:
            if os.path.exists(path) and os.path.isfile(path):
                os.remove(path)
                return
        dataframe.to_csv(path_or_buf=path, index=False, header=False)
