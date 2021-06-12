import numpy
import pandas
from .variables import VARIABLES


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
    output_file_path = VARIABLES["output_file_path"]
    for wrong_element in wrong_elements:
        wrong_columns = dataframe.isin([wrong_element]).sum().to_dict()
        for wrong_column in wrong_columns:
            if wrong_columns[wrong_column]:
                dataframe[wrong_column] = dataframe[wrong_column].replace(wrong_element, numpy.nan)
    dataframe.dropna(how='any', inplace=True)
    if VARIABLES["drop_duplicated"]:
        dataframe.drop_duplicates(inplace=True)
    dataframe.to_csv(output_file_path, index=False)


def initial_clean():
    """
    Concatenates data_file_path and test_file_path (_concatenate_files)
    and converts all fields on {dataframe} to string, them drops lines where
    it finds known wrong elements listed on known_wrong_elements. If
    drop_duplicated is True, drops all {dataframe} duplicated lines.
    Finally creates an output.csv file in data folder (_initial_clean_process)
    """
    dataframe = _concatenate_files()
    _initial_clean_process(dataframe)
