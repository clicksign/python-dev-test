import numpy
import pandas
from variables import VARIABLES


def number_of_columns_is(expected_number_of_columns: int) -> bool:
    data_file_path = VARIABLES["data_file_path"]
    adult_file_dataframe = pandas.read_csv(data_file_path, sep=',', header=None, )
    number_of_columns = len(adult_file_dataframe.columns)
    return number_of_columns == expected_number_of_columns


def clean_file():
    data_file_path = VARIABLES["data_file_path"]
    adult_file_dataframe = pandas.read_csv(data_file_path,
                                           skipinitialspace=True,
                                           sep=',',
                                           header=None,
                                           names=VARIABLES["expected_header"])
    adult_file_dataframe = adult_file_dataframe.astype(str)
    wrong_elements = VARIABLES["known_wrong_elements"]
    for wrong_element in wrong_elements:
        wrong_columns = adult_file_dataframe.isin([wrong_element]).sum(axis=0).to_dict()
        for wrong_column in wrong_columns:
            if wrong_columns[wrong_column]:
                adult_file_dataframe[wrong_column] = adult_file_dataframe[wrong_column].replace(wrong_element, numpy.nan)
    adult_file_dataframe.dropna(how='any', inplace=True)
    adult_file_dataframe.to_csv("data/output.csv")


def main():
    clean_file()


if __name__ == '__main__':
    main()
