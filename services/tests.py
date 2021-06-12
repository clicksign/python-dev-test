import os
import unittest
from variables import VARIABLES
from initial_clean import _number_of_columns_is, _is_able_to_parse


class FileTests(unittest.TestCase):

    def test_variables_integrity(self):
        entry_value = ["data_file_path", "test_file_path", "output_file_path",
                       "data_file_skip_row", "test_file_skip_row",
                       "number_of_columns", "expected_header",
                       "expected_values_and_types", "known_wrong_elements",
                       "drop_duplicated"]
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
        entry_value = VARIABLES["number_of_columns"]
        data_file_skip_row = VARIABLES["data_file_skip_row"]
        test_file_skip_row = VARIABLES["test_file_skip_row"]
        expected_value = True
        self.assertEqual(type(data_file_skip_row), int)
        self.assertEqual(type(test_file_skip_row), int)
        self.assertEqual(type(entry_value), int)
        self.assertEqual(_is_able_to_parse(), expected_value)
        self.assertEqual(_number_of_columns_is(entry_value), expected_value)

    def test_variables_columns_related(self):
        entry_value_number_of_columns = VARIABLES["number_of_columns"]
        entry_value_expected_header = VARIABLES["expected_header"]
        self.assertEqual(type(entry_value_number_of_columns), int)
        self.assertEqual(len(entry_value_expected_header), entry_value_number_of_columns)

    def test_variables_known_wrong_elements(self):
        entry_value = VARIABLES["known_wrong_elements"]
        self.assertEqual(type(entry_value), list)
        for element in entry_value:
            self.assertEqual(type(element), str)

    def test_variables_drop_duplicated(self):
        entry_value = VARIABLES["drop_duplicated"]
        self.assertEqual(type(entry_value), bool)


class StandaloneTests:

    @staticmethod
    def test_output_file_existence():
        pass

    @staticmethod
    def test_special_chars():
        pass

    @staticmethod
    def test_non_expected_values():
        pass

    @staticmethod
    def test_non_expected_types():
        pass


if __name__ == '__main__':
    unittest.main()
