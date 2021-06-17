import os
import unittest
import pandas as pd
from .variables import VARIABLES


class FileTests(unittest.TestCase):

    def test_output_file_existence(self):
        """
        Tests if the path on output_file_path value exists and if is a str
        """
        entry_value = VARIABLES["output_file_path"]
        expected_value = True
        self.assertEqual(type(entry_value), str)
        self.assertEqual(os.path.exists(entry_value), expected_value)


class StandaloneTests:

    @staticmethod
    def _is_able_to_parse() -> bool:
        """
        Verifies if data_file_path and test_file_path are able to parse through
        pd.read_csv
        @rtype: bool
        @return: a boolean representing parse viability
        """
        data_file_path = VARIABLES["data_file_path"]
        test_file_path = VARIABLES["test_file_path"]
        try:
            pd.read_csv(data_file_path,
                        sep=',',
                        header=None,
                        skiprows=VARIABLES["data_file_skip_row"], )
            pd.read_csv(test_file_path,
                        sep=',',
                        header=None,
                        skiprows=VARIABLES["test_file_skip_row"], )
            return True
        except pd.errors.ParserError:
            return False

    @staticmethod
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
        data_file_dataframe = pd.read_csv(data_file_path,
                                          sep=',',
                                          header=None,
                                          skiprows=VARIABLES["data_file_skip_row"], )
        test_file_dataframe = pd.read_csv(test_file_path,
                                          sep=',',
                                          header=None,
                                          skiprows=VARIABLES["test_file_skip_row"], )
        data_n_of_columns = len(data_file_dataframe.columns)
        test_n_of_columns = len(test_file_dataframe.columns)
        return data_n_of_columns == expected_number_of_columns and test_n_of_columns == expected_number_of_columns

    @staticmethod
    def test_non_expected_values(string: str, column_number: int) -> bool:
        """
        Verifies if {string} is a non-expected value of the {column_number} of expected_values_and_types
        @type string: str
        @type column_number: int
        @rtype: bool
        @param string: a string representing the supposedly non-expected value
        @param column_number: a int representing the column index related to expected_values_and_types
        @return: a boolean representing the non-expectation
        """
        expected_values_and_types = VARIABLES["expected_values_and_types"]
        expected_values = expected_values_and_types[column_number]
        return string in expected_values

    @staticmethod
    def test_non_expected_types(string: str, column_number: int) -> bool:
        """
        Verifies if {string} is a non-expected type of the {column_number} of expected_values_and_types
        @type string: str
        @type column_number: int
        @rtype: bool
        @param string: a string representing the supposedly non-expected type
        @param column_number: a int representing the column index related to expected_values_and_types
        @return: a boolean representing the non-expectation
        """
        expected_values_and_types = VARIABLES["expected_values_and_types"]
        expected_type = expected_values_and_types[column_number]
        try:
            expected_type(string)
            return True
        except ValueError:
            return False


if __name__ == '__main__':
    unittest.main()
