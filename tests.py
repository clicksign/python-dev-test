import os
import unittest
from variables import VARIABLES
from main import number_of_columns_is


class FileTests(unittest.TestCase):

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
        expected_value = True
        self.assertEqual(type(entry_value), int)
        self.assertEqual(number_of_columns_is(entry_value), expected_value)

    def test_variables_integrity(self):
        pass # Verify if all variables needed exists

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

    def test_special_chars(self):
        pass

    def test_non_expected_values(self):
        pass

    def test_non_expected_types(self):
        pass


if __name__ == '__main__':
    unittest.main()
