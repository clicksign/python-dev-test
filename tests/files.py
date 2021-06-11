import os
import unittest
from variables import VARIABLES


class FileTests(unittest.TestCase):

    def test_file_existence(self):
        """  """
        entry_value = VARIABLES["data_file_path"]
        expected_value = True
        self.assertEqual(os.path.exists(entry_value), expected_value)

    def test_number_of_columns(self):
        entry_value = VARIABLES["number_of_columns"]
        expected_value = True
        pass

    def test_header_integrity(self):
        entry_value = ["age", "workclass", "fnlwgt", "education",
                       "education-num", "marital-status", "occupation",
                       "relationship", "race", "sex", "capital-gain",
                       "capital-loss", "hours-per-week", "native-country",
                       "class"]
        expected_value = True
        pass


if __name__ == '__main__':
    unittest.main()
