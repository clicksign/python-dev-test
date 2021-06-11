import pandas
from variables import VARIABLES


def number_of_columns_is(expected_number_of_columns: int) -> bool:
    data_file_path = VARIABLES["data_file_path"]
    adult_file_dataframe = pandas.read_csv(data_file_path, sep=',', header=None, )
    number_of_columns = len(adult_file_dataframe.columns)
    return number_of_columns == expected_number_of_columns


def main():
    print(number_of_columns_is(15))


if __name__ == '__main__':
    main()
