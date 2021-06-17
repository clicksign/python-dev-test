import pandas as pd
from typing import Callable


def create_dataframe_thread(dataframe: pd.DataFrame, number_of_threads: int) -> list:
    """
    Splits the {dataframe} in dictionaries of rows based on records
    @type dataframe: pd.Dataframe
    @type number_of_threads: int
    @rtype: list
    @param dataframe: a dataframe representing the dataframe to be split
    @param number_of_threads: a integer representing the number of splits
    @return: a list of lists of dictionaries representing the dataframe rows as following:
    [[{"col1": "value_1", "col2": "value_2"}, {"col1": "value_1", "col2": "value_2"},],
    [{"col1": "value_1", "col2": "value_2"}, {"col1": "value_1", "col2": "value_2"},],]
    """
    dataframe_input_dict = dataframe.to_dict(orient='records')
    dataframes = []
    temp_rows_list = []
    for row in dataframe_input_dict:
        temp_rows_list.append(row)
    dataframe_size = len(temp_rows_list)
    if number_of_threads and dataframe_size <= number_of_threads:
        for temp_row in temp_rows_list:
            dataframes.append([temp_row])
    elif number_of_threads:
        threads_size = int(dataframe_size / number_of_threads)
        threads_rest = dataframe_size % number_of_threads
        if number_of_threads == 1:
            dataframes.append(temp_rows_list)
        elif threads_size == 1 and (threads_rest == 1 or threads_rest == 0):
            for temp_row in temp_rows_list:
                dataframes.append([temp_row])
        elif threads_size == 1:
            rest_thread = temp_rows_list[-threads_rest:]
            for temp_row in temp_rows_list[:-threads_rest]:
                dataframes.append([temp_row])
            for temp_row in rest_thread:
                position_index = rest_thread.index(temp_row)
                dataframes[position_index].append(temp_row)
        elif threads_rest:
            rest_thread = temp_rows_list[-threads_rest:]
            thread_start = 0
            thread_end = threads_size
            while thread_end <= len(temp_rows_list[:-threads_rest]):
                dataframes.append(temp_rows_list[thread_start:thread_end])
                thread_start = thread_end
                thread_end += threads_size
            for temp_row in rest_thread:
                position_index = rest_thread.index(temp_row)
                dataframes[position_index].append(temp_row)
        else:
            thread_start = 0
            thread_end = threads_size
            while thread_end <= len(temp_rows_list):
                dataframes.append(temp_rows_list[thread_start:thread_end])
                thread_start = thread_end
                thread_end += threads_size
    else:
        dataframes.append(temp_rows_list)
    return dataframes


def are_there_threads_alive(running_threads: list):
    """
    Verifies the existence of running threads
    @type running_threads: list
    @rtype: bool
    @param running_threads: a list of probably open and running threads
    @return: a boolean representing the existence of running threads
    """
    for running_thread in running_threads:
        if running_thread.is_alive():
            return True
    return False


def run_thread(variable: any, function: Callable):
    function(variable)
