import os
import shutil
import threading
import time
import pandas
from numpy.random import randint
from .standalone_tests import StandaloneTests
from .variables import VARIABLES
from .threads import create_dataframe_thread, run_thread, are_there_threads_alive


def _clean_outputs_folder() -> bool:
    """
    Deletes all files from "data/outputs/"
    @rtype: bool
    @return: a boolean representing the deletion success
    """
    outputs_path = os.path.join(os.getcwd(), "data", "outputs")
    if VARIABLES["verbosity"]:
        print(f"Deleting files from {outputs_path}!")
    try:
        outputs_files = os.listdir(outputs_path)
        outputs_files_exists = len(outputs_files) > 0
        if outputs_files_exists:
            for outputs_file in outputs_files:
                sources_file_path = os.path.join(outputs_path, outputs_file)
                is_file = os.path.isfile(sources_file_path)
                is_link = os.path.islink(sources_file_path)
                is_dir = os.path.isdir(sources_file_path)
                if is_file or is_link:
                    os.unlink(sources_file_path)
                elif is_dir:
                    shutil.rmtree(sources_file_path)
    except FileNotFoundError:
        os.mkdir(outputs_path)
    return True


def _create_csv(dataframe: pandas.DataFrame):
    """
    Creates a csv file using {dataframe} in data/outputs
    @param dataframe: a dataframe representing the file to be created
    """
    outputs_path = os.path.join(os.getcwd(), "data", "outputs")
    number = randint(0, 9999)
    name = f"{number}.csv"
    path = os.path.join(outputs_path, name)
    csv_file_exists = os.path.exists(path)
    while csv_file_exists:
        number = randint(0, 9999)
        name = f"{number}.csv"
        path = os.path.join(outputs_path, name)
        csv_file_exists = os.path.exists(path)
    else:
        if VARIABLES["verbosity"]:
            print(f"Creating {path}!")
        dataframe.to_csv(path, index=False)


def _clean_and_validate(dataframe_list: list):
    """
    Converts all fields on {dataframe} to string, then drops lines where
    it finds known wrong elements listed on known_wrong_elements and lines where
    test_non_expected_types and/or test_non_expected_values is False.
    Then replaces all unwelcome chars and/or words on its values.
    Finally creates a csv in data/outputs (_create_csv)
    @type dataframe_list: list
    @param dataframe_list: a list of dictionaries representing the rows to be cleaned
    and transformed into output.csv
    """
    expected_values_and_types = VARIABLES["expected_values_and_types"]
    expected_header = VARIABLES["expected_header"]
    unwelcome_chars_and_words = VARIABLES["unwelcome_chars_and_words"]
    wrong_elements = VARIABLES["known_wrong_elements"]
    dataframe = pandas.DataFrame.from_records(dataframe_list)
    dataframe = dataframe.astype(str)
    if VARIABLES["verbosity"]:
        print(f"Validating a list of records!")
    for wrong_element in wrong_elements:
        for column in dataframe.columns:
            drop_index = dataframe[dataframe[column] == wrong_element].index
            dataframe.drop(drop_index, inplace=True)
    for index, row in dataframe.iterrows():
        for column_number in expected_values_and_types:
            column_name = expected_header[column_number]
            string = row.to_dict()[column_name]
            is_type = type(expected_values_and_types[column_number]) is type
            if is_type:
                is_an_expected_type = StandaloneTests.test_non_expected_types(string, column_number)
                if not is_an_expected_type:
                    dataframe.drop(index, inplace=True)
                    break
            else:
                is_an_expected_value = StandaloneTests.test_non_expected_values(string, column_number)
                if not is_an_expected_value:
                    dataframe.drop(index, inplace=True)
                    break
            for unwelcome_char_and_word in unwelcome_chars_and_words:
                welcome_chars_and_words = unwelcome_chars_and_words[unwelcome_char_and_word]
                string = string.replace(unwelcome_char_and_word, welcome_chars_and_words)
            dataframe.loc[index, column_name] = string.strip()
    _create_csv(dataframe)


def clean_and_validate(dataframe: pandas.DataFrame):
    """
    Deletes all files in "data/outputs/" (_clean_outputs_folder),
    converts all fields on {dataframe} to string, then drops lines
    where it finds known wrong elements listed on known_wrong_elements.
    If drop_duplicated is True, drops all {dataframe} duplicated lines
    (_initial_clean_process). Then converts all fields on {dataframe} to string,
    drops lines where test_non_expected_types and/or test_non_expected_values is False.
    Then replaces all unwelcome chars and/or words on its values.
    Finally creates a csv on data/outputs for every thread.
    """
    number_of_threads = VARIABLES["number_of_threads"]
    drop_duplicated = VARIABLES["drop_duplicated"]
    outputs_folder_is_cleaned = _clean_outputs_folder()
    dataframe = dataframe.astype(str)
    if drop_duplicated:
        dataframe.drop_duplicates(inplace=True)
    dataframe_size = dataframe.shape[0]
    there_is_rows = dataframe_size > 0
    running_threads = []
    if there_is_rows:
        if outputs_folder_is_cleaned:
            dataframe_threads = create_dataframe_thread(dataframe, number_of_threads)
            for dataframe_thread in dataframe_threads:
                running_threads.append(threading.Thread(target=run_thread, args=(dataframe_thread,
                                                                                 _clean_and_validate)))
            for thread in running_threads:
                thread.start()
            while are_there_threads_alive(running_threads):
                time.sleep(0.1)
    else:
        if VARIABLES["verbosity"]:
            print(f"Cleaning and validation concluded!")
