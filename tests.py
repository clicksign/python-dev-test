import os
import unittest
import pandas as pd
from services.variables import VARIABLES
from services.standalone_tests import StandaloneTests
from services.threads import create_dataframe_thread


class FileTests(unittest.TestCase):

    def test_variables_integrity(self):
        """
         Tests all variables availability
        """
        entry_value = ["data_file_path", "test_file_path",
                       "data_file_skip_row", "test_file_skip_row",
                       "expected_number_of_columns", "expected_header",
                       "expected_values_and_types", "known_wrong_elements",
                       "drop_duplicated", "unwelcome_chars_and_words",
                       "number_of_threads", "verbosity", "run_every_seconds",
                       "processing_data_limit", "columns_to_be_analysed",
                       "analysis_relation"]
        expected_value = True
        for element in VARIABLES:
            self.assertEqual(element in entry_value, expected_value)

    def test_data_file_existence(self):
        """
        Tests if the path on data_file_path value exists and if is a str
        """
        entry_value = VARIABLES["data_file_path"]
        expected_value = True
        self.assertEqual(type(entry_value), str)
        self.assertEqual(os.path.exists(entry_value), expected_value)

    def test_file_existence(self):
        """
        Tests if the path on test_file_path value exists and if is a str
        """
        entry_value = VARIABLES["test_file_path"]
        expected_value = True
        self.assertEqual(type(entry_value), str)
        self.assertEqual(os.path.exists(entry_value), expected_value)

    def test_number_of_columns(self):
        """
        Tests if data_file_skip_row, test_file_skip_row and entry_value are positive ints
        Tests if pandas is able to parse data_file_path and test_file_path
        Tests if expected_number_of_columns matches the number of columns of data_file_path and test_file_path
        """
        entry_value_expected_number_of_columns = VARIABLES["expected_number_of_columns"]
        entry_value_data_file_skip_row = VARIABLES["data_file_skip_row"]
        entry_value_test_file_skip_row = VARIABLES["test_file_skip_row"]
        expected_value = True
        self.assertEqual(type(entry_value_data_file_skip_row), int)
        self.assertGreater(entry_value_data_file_skip_row, -1)
        self.assertEqual(type(entry_value_test_file_skip_row), int)
        self.assertGreater(entry_value_test_file_skip_row, -1)
        self.assertEqual(type(entry_value_expected_number_of_columns), int)
        self.assertGreater(entry_value_expected_number_of_columns, 0)
        self.assertEqual(StandaloneTests._is_able_to_parse(), expected_value)
        self.assertEqual(StandaloneTests._number_of_columns_is(entry_value_expected_number_of_columns), expected_value)

    def test_variables_columns_related(self):
        """
        Tests if expected_number_of_columns is a int, if expected_header is a list and
        if expected_values_and_types is a dict
        Tests if each expected_header value is a str
        Tests if each expected_values_and_types key is a int
        Tests if each expected_values_and_types value is a list or type
        Tests if expected_values_and_types length matches expected_number_of_columns
        Tests if expected_header length matches expected_number_of_columns
        """
        entry_value_expected_number_of_columns = VARIABLES["expected_number_of_columns"]
        entry_value_expected_header = VARIABLES["expected_header"]
        entry_value_expected_values_and_types = VARIABLES["expected_values_and_types"]
        self.assertEqual(type(entry_value_expected_number_of_columns), int)
        self.assertEqual(type(entry_value_expected_header), list)
        self.assertEqual(type(entry_value_expected_values_and_types), dict)
        for element in entry_value_expected_header:
            self.assertEqual(type(element), str)
        for element in entry_value_expected_values_and_types:
            self.assertEqual(type(element), int)
            self.assertTrue(isinstance(entry_value_expected_values_and_types[element],
                                       (list, type)))
            if type(entry_value_expected_values_and_types[element]) is list:
                for inner_element in entry_value_expected_values_and_types[element]:
                    self.assertEqual(type(inner_element), str)
        self.assertEqual(len(entry_value_expected_values_and_types), entry_value_expected_number_of_columns)
        self.assertEqual(len(entry_value_expected_header), entry_value_expected_number_of_columns)

    def test_variables_columns_to_be_analysed(self):
        """
        Tests if columns_to_be_analysed is a list
        Tests if each columns_to_be_analysed value is a str
        Tests if each columns_to_be_analysed value is in expected_header
        """
        entry_value_columns_to_be_analysed = VARIABLES["columns_to_be_analysed"]
        entry_value_expected_header = VARIABLES["expected_header"]
        self.assertEqual(type(entry_value_columns_to_be_analysed), list)
        for element in entry_value_columns_to_be_analysed:
            self.assertEqual(type(element), str)
            self.assertTrue(element in entry_value_expected_header)

    def test_variables_drop_duplicated(self):
        """
        Tests if drop_duplicated is a bool
        """
        entry_value = VARIABLES["drop_duplicated"]
        self.assertEqual(type(entry_value), bool)

    def test_variables_verbosity(self):
        """
        Tests if verbosity is a bool
        """
        entry_value = VARIABLES["verbosity"]
        self.assertEqual(type(entry_value), bool)

    def test_variables_run_every_seconds(self):
        """
        Tests if run_every_seconds is a int
        Tests if run_every_seconds value is greater than -1
        """
        entry_value = VARIABLES["run_every_seconds"]
        self.assertEqual(type(entry_value), int)
        self.assertGreater(entry_value, -1)

    def test_variables_processing_data_limit(self):
        """
        Tests if processing_data_limit is a int
        Tests if processing_data_limit value is greater than 0
        """
        entry_value = VARIABLES["processing_data_limit"]
        self.assertEqual(type(entry_value), int)
        self.assertGreater(entry_value, 0)

    def test_variables_unwelcome_chars_and_words(self):
        """
        Tests if unwelcome_chars_and_words is a dict
        Tests if unwelcome_chars_and_words value is greater than 0
        Tests if each unwelcome_chars_and_words key is a str
        Tests if each unwelcome_chars_and_words value is a str
        """
        entry_value = VARIABLES["unwelcome_chars_and_words"]
        self.assertEqual(type(entry_value), dict)
        for element in entry_value:
            self.assertEqual(type(element), str)
            self.assertEqual(type(entry_value[element]), str)

    def test_variables_number_of_threads(self):
        """
        Tests if number_of_threads is a int
        Tests if number_of_threads value is greater than 0
        """
        entry_value = VARIABLES["number_of_threads"]
        self.assertEqual(type(entry_value), int)
        self.assertGreater(entry_value, 0)

    def test_variables_analysis_relation(self):
        """
        Tests if analysis_relation is a dict
        Tests if analysis_relation key is a str
        Tests if analysis_relation keys is in ["two_grouped_bar"]
        For two_grouped_bar key:
            Tests if two_grouped_bar is a three length list
            Tests if two_grouped_bar first value is a list
            Tests if two_grouped_bar second value is a list
            Tests if two_grouped_bar third value is a str
            Tests if two_grouped_bar list values are str
            Tests if two_grouped_bar list first values are in expected_header
            Tests if two_grouped_bar list second values are in expected_values_and_types related column value
        """
        entry_value_analysis_relation = VARIABLES["analysis_relation"]
        entry_value_expected_header = VARIABLES["expected_header"]
        entry_value_expected_values_and_types = VARIABLES["expected_values_and_types"]
        expected_values = ["two_grouped_bar", "pie"]
        self.assertEqual(type(entry_value_analysis_relation), list)
        for element in entry_value_analysis_relation:
            self.assertEqual(type(element), list)
            self.assertTrue(element[0] in expected_values)
            if element[0] == "two_grouped_bar":
                self.assertEqual(type(element[1]), list)
                self.assertEqual(len(element[1]), 3)
                self.assertEqual(type(element[1][0]), list)
                self.assertEqual(type(element[1][1]), list)
                self.assertEqual(type(element[1][2]), str)
                self.assertEqual(type(element[1][0][0]), str)
                self.assertEqual(type(element[1][0][1]), str)
                self.assertEqual(type(element[1][1][0]), str)
                self.assertEqual(type(element[1][1][1]), str)
                self.assertTrue(element[1][0][0] in entry_value_expected_header)
                self.assertTrue(element[1][1][0] in entry_value_expected_header)
                self.assertTrue(element[1][2] in entry_value_expected_header)
                inner_element_0_index = entry_value_expected_header.index(element[1][0][0])
                inner_element_1_index = entry_value_expected_header.index(element[1][1][0])
                inner_element_2_index = entry_value_expected_header.index(element[1][2])
                self.assertEqual(type(entry_value_expected_values_and_types[inner_element_0_index]), list)
                self.assertEqual(type(entry_value_expected_values_and_types[inner_element_1_index]), list)
                self.assertEqual(type(entry_value_expected_values_and_types[inner_element_2_index]), list)
                self.assertTrue(element[1][0][1] in entry_value_expected_values_and_types[inner_element_0_index])
                self.assertTrue(element[1][1][1] in entry_value_expected_values_and_types[inner_element_1_index])
            if element[0] == "pie":
                self.assertEqual(type(element[1]), list)
                self.assertEqual(len(element[1]), 2)
                self.assertEqual(type(element[1][0]), list)
                self.assertTrue(isinstance(element[1][1], str) or element[1][1] is None)
                self.assertEqual(type(element[1][0][0]), str)
                self.assertEqual(type(element[1][0][1]), str)
                self.assertTrue(element[1][0][0] in entry_value_expected_header)
                self.assertTrue(element[1][0][1] in entry_value_expected_header)
                if isinstance(element[1][1], str):
                    element_index = entry_value_expected_header.index(element[1][0][0])
                    self.assertTrue(element[1][1] in entry_value_expected_values_and_types[element_index])

    def test_threading_with_one_row_one_thread(self):
        """
        Tests if dataframe with size of 1 with 1 thread returns 1 thread size
        """
        entry_value_dataframe = pd.DataFrame(data={'col1': [1], 'col2': [2]})
        entry_value_number_of_threads = 1
        expected_value = 1
        self.assertEqual(len(create_dataframe_thread(entry_value_dataframe,
                                                     entry_value_number_of_threads)
                             ),
                         expected_value)

    def test_threading_with_one_row_two_threads(self):
        """
        Tests if dataframe with size of 1 with 2 threads returns 1 thread size
        """
        entry_value_dataframe = pd.DataFrame(data={'col1': [1], 'col2': [2]})
        entry_value_number_of_threads = 2
        expected_value = 1
        self.assertEqual(len(create_dataframe_thread(entry_value_dataframe,
                                                     entry_value_number_of_threads)),
                         expected_value)

    def test_threading_with_two_rows_one_thread(self):
        """
        Tests if dataframe with size of 2 with 1 thread returns 1 thread size
        """
        entry_value_dataframe = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})
        entry_value_number_of_threads = 1
        expected_value = 1
        self.assertEqual(len(create_dataframe_thread(entry_value_dataframe,
                                                     entry_value_number_of_threads)),
                         expected_value)

    def test_threading_with_three_rows_two_threads(self):
        """
        Tests if dataframe with size of 3 with 2 threads returns 3 thread size
        """
        entry_value_dataframe = pd.DataFrame(data={'col1': [1, 2, 3], 'col2': [4, 5, 6]})
        entry_value_number_of_threads = 2
        expected_value = 3
        self.assertEqual(len(create_dataframe_thread(entry_value_dataframe,
                                                     entry_value_number_of_threads)),
                         expected_value)

    def test_threading_with_five_rows_three_threads(self):
        """
        Tests if dataframe with size of 5 with 3 threads returns 3 thread size
        """
        entry_value_dataframe = pd.DataFrame(data={'col1': [1, 2, 3, 4, 5], 'col2': [6, 7, 8, 9, 10]})
        entry_value_number_of_threads = 3
        expected_value = 3
        self.assertEqual(len(create_dataframe_thread(entry_value_dataframe,
                                                     entry_value_number_of_threads)),
                         expected_value)


if __name__ == '__main__':
    unittest.main()
