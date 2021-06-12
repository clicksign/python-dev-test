import numpy
import pandas
from variables import VARIABLES


def is_able_to_parse() -> bool:
    data_file_path = VARIABLES["data_file_path"]
    test_file_path = VARIABLES["test_file_path"]
    try:
        pandas.read_csv(data_file_path,
                        sep=',',
                        header=None,
                        skiprows=VARIABLES["data_file_skip_row"], )
        pandas.read_csv(test_file_path,
                        sep=',',
                        header=None,
                        skiprows=VARIABLES["test_file_skip_row"], )
        return True
    except pandas.errors.ParserError:
        return False


def number_of_columns_is(expected_number_of_columns: int) -> bool:
    data_file_path = VARIABLES["data_file_path"]
    test_file_path = VARIABLES["test_file_path"]
    data_file_dataframe = pandas.read_csv(data_file_path,
                                          sep=',',
                                          header=None,
                                          skiprows=VARIABLES["data_file_skip_row"], )
    test_file_dataframe = pandas.read_csv(test_file_path,
                                          sep=',',
                                          header=None,
                                          skiprows=VARIABLES["test_file_skip_row"], )
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
                                          names=VARIABLES["expected_header"], )
    test_file_dataframe = pandas.read_csv(test_file_path,
                                          skipinitialspace=True,
                                          sep=',',
                                          header=None,
                                          names=VARIABLES["expected_header"], )
    print(test_file_dataframe)

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
