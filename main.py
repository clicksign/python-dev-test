import os
import sqlite3
import sys
import pandas
import tests
import unittest
from services.variables import VARIABLES
from services.sqlite import sqlite_erase_from, sqlite_get_dataframe_from, sqlite_create_table_on
from services.clean import clean_and_validate


def concatenate_outputs() -> pandas.DataFrame:
    """
    Concatenates all files in "data/outputs/"
    @rtype: pandas.DataFrame
    @return: a dataframe representing the concatenation all files in "data/outputs/"
    """
    outputs_path = os.path.join(os.getcwd(), "data", "outputs")
    outputs_files = os.listdir(outputs_path)
    dataframes = []
    outputs_files_exists = len(outputs_files) > 1
    if outputs_files_exists:
        for outputs_file in outputs_files:
            outputs_file_path = os.path.join(outputs_path, outputs_file)
            outputs_dataframe = pandas.read_csv(outputs_file_path)
            dataframes.append(outputs_dataframe)
    return pandas.concat(dataframes)


def concatenate_files() -> pandas.DataFrame:
    """
    Concatenates data_file_path and test_file_path
    @rtype: pandas.DataFrame
    @return: a dataframe representing the concatenation of data_file_path
    and test_file_path
    """
    if VARIABLES["verbosity"]:
        print(f"Concatenating files!")
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
    dataframe = pandas.concat([data_file_dataframe, test_file_dataframe])
    return dataframe


def process_data_from(dataframe: pandas.DataFrame, periodically: bool):
    with sqlite3.connect("SQLite_ClickSign.db") as connection:
        sqlite_dataframe = sqlite_get_dataframe_from(connection, "data")
    sqlite_dataframe_size = sqlite_dataframe.shape[0]
    dataframe_size = dataframe.shape[0]
    processing_data_limit = VARIABLES["processing_data_limit"]
    is_close_to_finish = sqlite_dataframe_size + processing_data_limit >= dataframe_size
    if is_close_to_finish:
        dataframe_partial = dataframe[sqlite_dataframe_size:]
    else:
        dataframe_partial = dataframe[sqlite_dataframe_size:sqlite_dataframe_size + processing_data_limit]
    clean_and_validate(dataframe_partial)
    dataframe_outputs = concatenate_outputs()
    if sqlite_dataframe.empty:
        dataframe_sqlite = dataframe_outputs
    else:
        dataframe_sqlite = pandas.concat([sqlite_dataframe, dataframe_outputs])
    print(dataframe_sqlite)
    sqlite_create_table_on(dataframe_sqlite, "data")


def main():
    try:
        action = sys.argv[1]
        if action in ["-t", "--test", ]:
            suite = unittest.TestLoader().loadTestsFromModule(tests)
            unittest.TextTestRunner(verbosity=2).run(suite)
        elif action in ["-s", "--start", "-p", "--proceed", "-ot", "--one-time", ]:
            if action in ["-s", "--start", ]:
                sqlite_erase_from("data")
            dataframe = concatenate_files()
            if action in ["-s", "--start", "-p", "--proceed", ]:
                process_data_from(dataframe, periodically=True)
            else:
                process_data_from(dataframe, periodically=False)
        else:
            raise IndexError
    except IndexError:
        print("SyntaxError: This is NOT a valid syntax.")
        print("Please use the following:")
        print("-t | --test        Tests variables and other functions to process data")
        print("-s | --start       Start from scratch")
        print("-p | --proceed     Continue where you left off")
        print("-ot | --one-time   Continue where you left off just once")
        print("main.py [-t | --test | -s | --start | -p | --proceed | -ot | --one-time]")


if __name__ == '__main__':
    main()
