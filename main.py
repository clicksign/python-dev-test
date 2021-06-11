import pandas
from variables import VARIABLES


def number_of_columns_is(expected_number_of_columns: int) -> bool:
    data_file_path = VARIABLES["data_file_path"]
    adult_file_dataframe = pandas.read_csv(data_file_path, sep=',', header=None, )
    number_of_columns = len(adult_file_dataframe.columns)
    return number_of_columns == expected_number_of_columns


def header_is(expected_header_items: list) -> bool:
    pass


def clean_file():
    pass


def main():
    data_file_path = VARIABLES["data_file_path"]
    adult_file_dataframe = pandas.read_csv(data_file_path, sep=',', header=None, )
    adult_file_dataframe.info()


if __name__ == '__main__':
    main()
