from typing import Any

import numpy
import pandas
from variables import VARIABLES


def number_of_columns_is(expected_number_of_columns: int) -> bool:
    data_file_path = VARIABLES["data_file_path"]
    test_file_path = VARIABLES["test_file_path"]
    try:
        try:
            data_file_dataframe = pandas.read_csv(data_file_path, sep=',', header=None, )
        except pandas.errors.ParserError:
            data_file_dataframe = pandas.read_csv(data_file_path, sep=',', header=None, skiprows=1, )
        try:
            test_file_dataframe = pandas.read_csv(test_file_path, sep=',', header=None, )
        except pandas.errors.ParserError:
            test_file_dataframe = pandas.read_csv(test_file_path, sep=',', header=None, skiprows=1, )
    except not pandas.errors.ParserError:
        return False
    data_number_of_columns = len(data_file_dataframe.columns)
    test_number_of_columns = len(test_file_dataframe.columns)
    return data_number_of_columns == expected_number_of_columns and test_number_of_columns == expected_number_of_columns


def concatenate_file() -> pandas.DataFrame:
    data_file_path = VARIABLES["data_file_path"]
    test_file_path = VARIABLES["test_file_path"]
    data_file_dataframe = pandas.read_csv(data_file_path,
                                          skipinitialspace=True,
                                          sep=',',
                                          header=None,
                                          names=VARIABLES["expected_header"])
    print(data_file_dataframe[data_file_dataframe.duplicated()])

    return data_file_dataframe


def initial_clean_process(dataframe: pandas.DataFrame):
    dataframe = dataframe.astype(str)
    wrong_elements = VARIABLES["known_wrong_elements"]
    for wrong_element in wrong_elements:
        wrong_columns = dataframe.isin([wrong_element]).sum().to_dict()
        for wrong_column in wrong_columns:
            if wrong_columns[wrong_column]:
                dataframe[wrong_column] = dataframe[wrong_column].replace(wrong_element, numpy.nan)
    dataframe.dropna(how='any', inplace=True)
    if VARIABLES["drop_duplicated"]:
        dataframe.drop_duplicates(inplace=True)
    dataframe.to_csv("data/output.csv")


def main():
    dataframe = concatenate_file()
    initial_clean_process(dataframe)


if __name__ == '__main__':
    main()
