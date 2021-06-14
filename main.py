import os
import sys
import pandas
import tests
import unittest
from services.clean import clean
from services.variables import VARIABLES
from services.sqlite import create_sqlite_table_from


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


def main():
    try:
        action = sys.argv[1]
        if action == "-t" or action == "--test":
            suite = unittest.TestLoader().loadTestsFromModule(tests)
            unittest.TextTestRunner(verbosity=2).run(suite)
        elif action == "-s" or action == "--start":
            # Reseta a tabela

            # Coleta data
            dataframe = concatenate_files()

            ## Ciclo
            # Coleta o conteúdo da tabela
            get_table_from

            # Coleta janela de dados pra ser limpa

            # Limpa a janela de data

            # Junta os csvs



            # Concatena os csv juntos com o conteúdo da tabela

            # Carrega no bd

            ## Fim do ciclo
        elif action == "-p" or action == "--proceed":
            pass
    except IndexError:
        print("SyntaxError: This is NOT a valid syntax.")
        print("Please use the following:")
        print("-t | --test      Tests variables and other functions to perform extraction")
        print("-s | --start     Start extraction based on variables")
        print("-p | --proceed   Proceed the last extraction")
        print("main.py [-t | --test | -s | --start | -p | --proceed]")


if __name__ == '__main__':
    main()
