import numpy
import pandas
from variables import VARIABLES


def _is_able_to_parse() -> bool:
    """
    Verifies if data_file_path and test_file_path are able to parse through
    pandas.read_csv
    @rtype: bool
    @return: a boolean representing parse viability
    """
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


def _number_of_columns_is(expected_number_of_columns: int) -> bool:
    """
    Verifies if data_file_path and test_file_path has the expected number
    of columns
    @param expected_number_of_columns: an integer representing expected number
    of columns
    @type expected_number_of_columns: int
    @rtype: bool
    @return: a boolean representing the data_file_path and test_file_path
    expected number of columns conformity
    """
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


def _concatenate_files() -> pandas.DataFrame:
    """
    Concatenates data_file_path and test_file_path
    @rtype: pandas.DataFrame
    @return: a dataframe representing the concatenation of data_file_path
    and test_file_path
    """
    data_file_path = VARIABLES["data_file_path"]
    test_file_path = VARIABLES["test_file_path"]
    data_file_dataframe = pandas.read_csv(data_file_path,
                                          skipinitialspace=True,
                                          sep=',',
                                          header=None,
                                          names=VARIABLES["expected_header"],
                                          skiprows=VARIABLES["data_file_skip_row"], )
    data_file_dataframe_size = data_file_dataframe.shape[0]
    test_file_dataframe = pandas.read_csv(test_file_path,
                                          skipinitialspace=True,
                                          sep=',',
                                          header=None,
                                          names=VARIABLES["expected_header"],
                                          skiprows=VARIABLES["test_file_skip_row"], )
    test_file_dataframe.index += data_file_dataframe_size
    return pandas.concat([data_file_dataframe, test_file_dataframe])


def _initial_clean_process(dataframe: pandas.DataFrame):
    """
    Converts all fields on {dataframe} to string, them drops lines where
    it finds known wrong elements listed on known_wrong_elements. If
    drop_duplicated is True, drops all {dataframe} duplicated lines.
    Finally creates an output.csv file in data folder
    @param dataframe: a dataframe representing the data to be cleaned
    and transformed into output.csv
    @type dataframe: pandas.DataFrame
    """
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
    dataframe.to_csv("data/output.csv", index=False)


def _initial_clean():
    """
    Concatenates data_file_path and test_file_path (_concatenate_files)
    and converts all fields on {dataframe} to string, them drops lines where
    it finds known wrong elements listed on known_wrong_elements. If
    drop_duplicated is True, drops all {dataframe} duplicated lines.
    Finally creates an output.csv file in data folder (_initial_clean_process)
    """
    dataframe = _concatenate_files()
    _initial_clean_process(dataframe)
