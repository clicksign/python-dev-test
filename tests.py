import os
import unittest
from variables import VARIABLES
from main import number_of_columns_is


class FileTests(unittest.TestCase):

    def test_file_existence(self):
        entry_value = VARIABLES["data_file_path"]
        expected_value = True
        self.assertEqual(os.path.exists(entry_value), expected_value)

    def test_number_of_columns(self):
        entry_value = VARIABLES["number_of_columns"]
        expected_value = True
        self.assertEqual(number_of_columns_is(entry_value), expected_value)


if __name__ == '__main__':
    unittest.main()
