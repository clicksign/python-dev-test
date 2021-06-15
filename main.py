import os
import sched
import sqlite3
import sys
import time
import pandas
import tests
import unittest
from services.variables import VARIABLES
from services.sqlite import sqlite_get_dataframe_from, sqlite_table_exists, sqlite_erase_create_or_update_from
from services.clean import clean_and_validate


def concatenate_outputs() -> pandas.DataFrame:
    """
    Concatenates all files in "data/outputs/"
    @rtype: pandas.DataFrame
    @return: a dataframe representing the concatenation all files in "data/outputs/"
    """
    outputs_path = os.path.join(os.getcwd(), "data", "outputs")
    outputs_files = os.listdir(outputs_path)
    expected_header = VARIABLES["expected_header"]
    dataframe = pandas.DataFrame([], columns=expected_header)
    dataframes = []
    outputs_files_exists = len(outputs_files) > 0
    if outputs_files_exists:
        for outputs_file in outputs_files:
            outputs_file_path = os.path.join(outputs_path, outputs_file)
            outputs_dataframe = pandas.read_csv(outputs_file_path)
            dataframes.append(outputs_dataframe)
        return pandas.concat(dataframes)
    return dataframe


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


def process_data_from(dataframe: pandas.DataFrame, periodically: bool, scheduler: sched.scheduler):
    with sqlite3.connect("SQLite_ClickSign.db") as connection:
        data_table_exists = sqlite_table_exists(connection, "data")
        if not data_table_exists:
            sqlite_erase_create_or_update_from("data")
        config_table_exists = sqlite_table_exists(connection, "config")
        if not config_table_exists:
            config_dataframe = pandas.DataFrame([0], columns=["processed rows"])
            sqlite_erase_create_or_update_from("config", config_dataframe)
        sqlite_data_dataframe = sqlite_get_dataframe_from(connection, "data")
        sqlite_config_dataframe = sqlite_get_dataframe_from(connection, "config")
    processed_data = sqlite_config_dataframe.loc[0]["processed rows"]
    dataframe_size = dataframe.shape[0]
    processing_data_limit = VARIABLES["processing_data_limit"]
    future_processed_data = processed_data + processing_data_limit
    is_close_to_finish = future_processed_data >= dataframe_size
    if is_close_to_finish:
        dataframe_partial = dataframe[processed_data:]
        periodically = False
    else:
        dataframe_partial = dataframe[processed_data:future_processed_data]
    clean_and_validate(dataframe_partial)
    dataframe_outputs = concatenate_outputs()
    print(f"Processing {processing_data_limit} rows!")
    print(f"{dataframe_outputs.shape[0]} are compliance!")
    if sqlite_data_dataframe.empty:
        dataframe_sqlite = dataframe_outputs
    else:
        dataframe_sqlite = pandas.concat([sqlite_data_dataframe, dataframe_outputs])
    difference_between_dataframes = pandas.concat([dataframe_sqlite,
                                                  sqlite_data_dataframe]).drop_duplicates(keep=False)
    if not dataframe_sqlite.empty:
        there_is_no_difference_between_dataframes = difference_between_dataframes.empty
        if there_is_no_difference_between_dataframes:
            answer = input("All data seems processed! Are you sure to proceed? (Y/N): ")
            if answer.lower() in ["n", "no"]:
                return
        sqlite_erase_create_or_update_from("data", dataframe_sqlite)
    config_dataframe = pandas.DataFrame([future_processed_data], columns=["processed rows"])
    sqlite_erase_create_or_update_from("config", config_dataframe)
    if periodically:
        run_every_seconds = VARIABLES["run_every_seconds"]
        if run_every_seconds:
            print(f"Waiting {run_every_seconds} seconds...")
            print("")
        scheduler.enter(run_every_seconds, 1, process_data_from, (dataframe, periodically, scheduler,))


def main():
    try:
        action = sys.argv[1]
        try:
            additional_action = sys.argv[2]
        except IndexError:
            additional_action = None
        print("Data processing started. Use CTRL^C to stop!")
        if action in ["-t", "--test", ]:
            principal_suite = unittest.TestLoader().loadTestsFromModule(tests)
            unittest.TextTestRunner(verbosity=2).run(principal_suite)
        elif action in ["-s", "--start", "-p", "--proceed", ]:
            if action in ["-s", "--start", ]:
                sqlite_erase_create_or_update_from("data")
                config_dataframe = pandas.DataFrame([0], columns=["processed rows"])
                sqlite_erase_create_or_update_from("config", config_dataframe)
            dataframe = concatenate_files()
            scheduler = sched.scheduler(time.time, time.sleep)
            if additional_action in ["-ot", "--one-time", ]:
                scheduler.enter(0, 1, process_data_from, (dataframe, False, scheduler,))
            else:
                scheduler.enter(0, 1, process_data_from, (dataframe, True, scheduler,))
            scheduler.run()
        else:
            raise IndexError
    except IndexError:
        print("SyntaxError: This is NOT a valid syntax.")
        print("Please use the following:")
        print("-t  | --test       Tests variables and other functions to process data")
        print("-s  | --start      Start from scratch")
        print("-p  | --proceed    Continue where you left off")
        print("-ot | --one-time   Run just once")
        print("main.py <-t | --test | -s | --start | -p | --proceed> [-ot | --one-time]")
    except KeyboardInterrupt:
        print("Data processing stopped with CTRL^C.")
        print("Use <-p | --proceed> to continue where you left off!")


if __name__ == '__main__':
    main()
