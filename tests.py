import os
import unittest
import pandas
from services.variables import VARIABLES
from services.standalone_tests import StandaloneTests
from services.threads import create_dataframe_thread


class FileTests(unittest.TestCase):

    def test_variables_integrity(self):
        entry_value = ["data_file_path", "test_file_path", "output_file_path",
                       "data_file_skip_row", "test_file_skip_row",
                       "expected_number_of_columns", "expected_header",
                       "expected_values_and_types", "known_wrong_elements",
                       "drop_duplicated", "unwelcome_chars_and_words",
                       "number_of_threads", "verbosity"]
        expected_value = True
        for element in VARIABLES:
            self.assertEqual(element in entry_value, expected_value)

    def test_data_file_existence(self):
        entry_value = VARIABLES["data_file_path"]
        expected_value = True
        self.assertEqual(type(entry_value), str)
        self.assertEqual(os.path.exists(entry_value), expected_value)

    def test_file_existence(self):
        entry_value = VARIABLES["test_file_path"]
        expected_value = True
        self.assertEqual(type(entry_value), str)
        self.assertEqual(os.path.exists(entry_value), expected_value)

    def test_number_of_columns(self):
        entry_value = VARIABLES["expected_number_of_columns"]
        data_file_skip_row = VARIABLES["data_file_skip_row"]
        test_file_skip_row = VARIABLES["test_file_skip_row"]
        expected_value = True
        self.assertEqual(type(data_file_skip_row), int)
        self.assertEqual(type(test_file_skip_row), int)
        self.assertEqual(type(entry_value), int)
        self.assertEqual(StandaloneTests._is_able_to_parse(), expected_value)
        self.assertEqual(StandaloneTests._number_of_columns_is(entry_value), expected_value)

    def test_variables_columns_related(self):
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
        self.assertEqual(len(entry_value_expected_values_and_types), entry_value_expected_number_of_columns)
        self.assertEqual(len(entry_value_expected_header), entry_value_expected_number_of_columns)

    def test_variables_drop_duplicated(self):
        entry_value = VARIABLES["drop_duplicated"]
        self.assertEqual(type(entry_value), bool)

    def test_variables_verbosity(self):
        entry_value = VARIABLES["verbosity"]
        self.assertEqual(type(entry_value), bool)

    def test_variables_unwelcome_chars_and_words(self):
        entry_value = VARIABLES["unwelcome_chars_and_words"]
        self.assertEqual(type(entry_value), dict)
        for element in entry_value:
            self.assertEqual(type(element), str)
            self.assertEqual(type(entry_value[element]), str)

    def test_variables_number_of_threads(self):
        entry_value = VARIABLES["number_of_threads"]
        self.assertEqual(type(entry_value), int)
        self.assertGreater(entry_value, 0)

    def test_threading_with_one_row_one_thread(self):
        entry_value_dataframe = pandas.DataFrame(data={'col1': [1], 'col2': [2]})
        entry_value_number_of_threads = 1
        expected_value = 1
        self.assertEqual(len(create_dataframe_thread(entry_value_dataframe,
                                                     entry_value_number_of_threads)
                             ),
                         expected_value)

    def test_threading_with_one_row_two_threads(self):
        entry_value_dataframe = pandas.DataFrame(data={'col1': [1], 'col2': [2]})
        entry_value_number_of_threads = 2
        expected_value = 1
        self.assertEqual(len(create_dataframe_thread(entry_value_dataframe,
                                                     entry_value_number_of_threads)),
                         expected_value)

    def test_threading_with_two_rows_one_thread(self):
        entry_value_dataframe = pandas.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})
        entry_value_number_of_threads = 1
        expected_value = 1
        self.assertEqual(len(create_dataframe_thread(entry_value_dataframe,
                                                     entry_value_number_of_threads)),
                         expected_value)

    def test_threading_with_three_rows_two_threads(self):
        entry_value_dataframe = pandas.DataFrame(data={'col1': [1, 2, 3], 'col2': [4, 5, 6]})
        entry_value_number_of_threads = 2
        expected_value = 3
        self.assertEqual(len(create_dataframe_thread(entry_value_dataframe,
                                                     entry_value_number_of_threads)),
                         expected_value)

    def test_threading_with_five_rows_three_threads(self):
        entry_value_dataframe = pandas.DataFrame(data={'col1': [1, 2, 3, 4, 5], 'col2': [6, 7, 8, 9, 10]})
        entry_value_number_of_threads = 3
        expected_value = 3
        self.assertEqual(len(create_dataframe_thread(entry_value_dataframe,
                                                     entry_value_number_of_threads)),
                         expected_value)


if __name__ == '__main__':
    unittest.main()
