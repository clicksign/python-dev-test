import os
import sched
import sqlite3
import sys
import time
import pandas as pd
import tests
import unittest
from services.variables import VARIABLES
from services.sqlite import sqlite_get_dataframe_from, sqlite_table_exists, sqlite_erase_create_or_update_from
from services.clean import clean_and_validate
from services.analyse import create_analysis_folder, graph_dispatcher, create_html_from_to


def concatenate_outputs() -> pd.DataFrame:
    """
    Concatenates all files in "data/outputs/"
    @rtype: pd.DataFrame
    @return: a dataframe representing the concatenation all files in "data/outputs/"
    """
    outputs_path = os.path.join(os.getcwd(), "data", "outputs")
    outputs_files = os.listdir(outputs_path)
    expected_header = VARIABLES["expected_header"]
    dataframe = pd.DataFrame([], columns=expected_header)
    dataframes = []
    outputs_files_exists = len(outputs_files) > 0
    if outputs_files_exists:
        for outputs_file in outputs_files:
            outputs_file_path = os.path.join(outputs_path, outputs_file)
            outputs_dataframe = pd.read_csv(outputs_file_path)
            dataframes.append(outputs_dataframe)
        return pd.concat(dataframes)
    return dataframe


def concatenate_files() -> pd.DataFrame:
    """
    Concatenates data_file_path and test_file_path
    @rtype: pd.DataFrame
    @return: a dataframe representing the concatenation of data_file_path
    and test_file_path
    """
    if VARIABLES["verbosity"]:
        print(f"Concatenating files!")
    data_file_path = VARIABLES["data_file_path"]
    test_file_path = VARIABLES["test_file_path"]
    names = VARIABLES["expected_header"]
    data_file_skip_row = VARIABLES["data_file_skip_row"]
    test_file_skip_row = VARIABLES["test_file_skip_row"]
    data_file_dataframe = pd.read_csv(data_file_path,
                                      skipinitialspace=True,
                                      sep=',',
                                      header=None,
                                      names=names,
                                      skiprows=data_file_skip_row, )
    data_file_dataframe_size = data_file_dataframe.shape[0]
    test_file_dataframe = pd.read_csv(test_file_path,
                                      skipinitialspace=True,
                                      sep=',',
                                      header=None,
                                      names=names,
                                      skiprows=test_file_skip_row, )
    test_file_dataframe.index += data_file_dataframe_size
    dataframe = pd.concat([data_file_dataframe, test_file_dataframe])
    return dataframe


def process_data_from(dataframe: pd.DataFrame, periodically: bool, scheduler: sched.scheduler):
    """
    Collects data from SQLite database, determines the amount of data that will be processed based
    on {dataframe}, processed rows gathered from config table and processing data limit.
    If data process is nearly concluded, change {periodically} to False.
    Converts all fields on {dataframe} to string, then drops lines where
    it finds known wrong elements listed on known_wrong_elements. If drop_duplicated is True,
    drops all {dataframe} duplicated lines. (clean_and_validate)
    Concatenates all files in "data/outputs/" (concatenate_outputs) then verifies if its presents
    any modification regarding data table. If presents, updates SQLite database and update table
    config processed rows.
    If {periodically}, reschedule this function.
    @type dataframe: pd.Dataframe
    @type periodically: bool
    @type scheduler: sched.scheduler
    @param dataframe: a dataframe representing the data
    @param periodically:
    @param scheduler:
    """
    processing_data_limit = VARIABLES["processing_data_limit"]
    run_every_seconds = VARIABLES["run_every_seconds"]
    dataframe_size = dataframe.shape[0]
    with sqlite3.connect("SQLite_ClickSign.db") as connection:
        data_table_exists = sqlite_table_exists(connection, "data")
        config_table_exists = sqlite_table_exists(connection, "config")
        if not data_table_exists:
            sqlite_erase_create_or_update_from("data")
        if not config_table_exists:
            config_dataframe = pd.DataFrame([0], columns=["processed rows"])
            sqlite_erase_create_or_update_from("config", config_dataframe)
        sqlite_data_dataframe = sqlite_get_dataframe_from(connection, "data")
        sqlite_config_dataframe = sqlite_get_dataframe_from(connection, "config")
    processed_rows = sqlite_config_dataframe.loc[0]["processed rows"]
    future_processed_data = processed_rows + processing_data_limit
    is_close_to_finish = future_processed_data >= dataframe_size
    if is_close_to_finish:
        dataframe_partial = dataframe[processed_rows:]
        periodically = False
    else:
        dataframe_partial = dataframe[processed_rows:future_processed_data]
    print(f"Processing {dataframe_partial.shape[0]} rows!")
    clean_and_validate(dataframe_partial)
    dataframe_outputs = concatenate_outputs()
    print(f"{dataframe_outputs.shape[0]} are compliance!")
    if sqlite_data_dataframe.empty:
        dataframe_sqlite = dataframe_outputs
    else:
        dataframe_sqlite = pd.concat([sqlite_data_dataframe, dataframe_outputs])
    difference_between_dataframes = pd.concat([dataframe_sqlite,
                                               sqlite_data_dataframe]).drop_duplicates(keep=False)
    if not dataframe_sqlite.empty:
        there_is_no_difference_between_dataframes = difference_between_dataframes.empty
        if there_is_no_difference_between_dataframes:
            return
        sqlite_erase_create_or_update_from("data", dataframe_sqlite)
    config_dataframe = pd.DataFrame([future_processed_data], columns=["processed rows"])
    sqlite_erase_create_or_update_from("config", config_dataframe)
    if periodically:
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
            dataframe = concatenate_files()
            scheduler = sched.scheduler(time.time, time.sleep)
            if action in ["-s", "--start", ]:
                sqlite_erase_create_or_update_from("data")
                config_dataframe = pd.DataFrame([0], columns=["processed rows"])
                sqlite_erase_create_or_update_from("config", config_dataframe)
            if additional_action in ["-ot", "--one-time", ]:
                scheduler.enter(0, 1, process_data_from, (dataframe, False, scheduler,))
            else:
                scheduler.enter(0, 1, process_data_from, (dataframe, True, scheduler,))
            scheduler.run()
        elif action in ["-a", "--analyse", ]:
            context = {"graph_images": {}}
            analysis_folder_path = create_analysis_folder()
            analysis_folder_graph_path = os.path.join(analysis_folder_path, "graphs")
            graph_dispatcher(analysis_folder_path)
            graphs_type_dirs = os.listdir(analysis_folder_graph_path)
            graphs_dict = {}
            for graphs_type_dir in graphs_type_dirs:
                graphs_dirs = os.path.join(analysis_folder_graph_path, graphs_type_dir)
                graphs = os.listdir(graphs_dirs)
                for graph in graphs:
                    graph_path = os.path.join(graphs_dirs, graph)
                    graph_path_start = graph_path.rfind("\\graphs") + 1
                    graph_path = graph_path[graph_path_start:]
                    print(graph_path)
                    graphs_dict[graph] = graph_path
            title_start = analysis_folder_path.rfind("\\") + 1
            context["title"] = analysis_folder_path[title_start:]
            context["images_list"] = list(graphs_dict)
            context["images_path_list"] = list(graphs_dict.values())
            context["number_of_graphs"] = len(graphs_dict)
            context["rest"] = len(graphs_dict) % 2
            create_html_from_to(context, analysis_folder_path)
        else:
            raise IndexError
    except IndexError:
        print("SyntaxError: This is NOT a valid syntax.")
        print("Please use the following:")
        print("-t  | --test       Tests variables and other functions to process data")
        print("-s  | --start      Start from scratch")
        print("-p  | --proceed    Continue where you left off")
        print("-ot | --one-time   Run just once")
        print("-a  | --analyse    Perform a SQLite content analysis")
        print("main.py <-t | --test | -s | --start | -p | --proceed | -a  | --analyse> [-ot | --one-time]")
    except KeyboardInterrupt:
        print("Data processing stopped with CTRL^C.")
        print("Use <-p | --proceed> to continue where you left off!")


if __name__ == '__main__':
    main()
