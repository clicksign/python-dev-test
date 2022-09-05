from distutils import errors
import sys
sys.path.append('../')

import unittest
from etl_clicksign.models import Adult
from etl_clicksign.algorithms import AdultExtraction
from etl_clicksign.__init__ import __version__
import pandas as pd

class TestSomeThings(unittest.TestCase):

    def test_version(self):
        assert __version__ == '0.1.0'

    def test_create_adult_table(self):
        Adult.create_table()
        assert Adult

    def test_insert_adult_table(self):
        mock_adult_insert = {
            "age" : ' ', 
            "workclass" : "State-gov", 
            "fnlwgt" : 77516, 
            "education" : "Bachelors", 
            "education_num" : 13, 
            "marital_status" : "Never-married", 
            "occupation" : "Adm-clerical", 
            "relationship" : "Not-in-family", 
            "race" : "White", 
            "sex" : "Male", 
            "capital_gain" : 2174, 
            "capital_loss" : 0, 
            "hours_per_week" : 40, 
            "native_country" : "United-States", 
            "_class" : "<=50K"
        }

        Adult.create_table()
        rec1=Adult.create(**mock_adult_insert)
        assert Adult.select().count() > 0

    def test_read_file_to_dataframe(self):
        columns_df = ["age", "workclass", "fnlwgt", "education", "education_num", "marital_status", "occupation",\
            "relationship", "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country",\
            "_class"
        ]
        
        df = pd.read_csv('Mock_Adult.data', skipinitialspace = True, delimiter = ',', names=columns_df)

        df["age"] = pd.to_numeric(df["age"], errors='coerce')
        df["fnlwgt"] = pd.to_numeric(df["fnlwgt"], errors='coerce')
        df["education_num"] = pd.to_numeric(df["education_num"], errors='coerce')
        df["capital_gain"] = pd.to_numeric(df["capital_gain"], errors='coerce')
        df["capital_loss"] = pd.to_numeric(df["capital_loss"], errors='coerce')
        df["hours_per_week"] = pd.to_numeric(df["hours_per_week"], errors='coerce')

        assert len([tuple(r) for r in df[:10].to_numpy()]) == 10
        assert len(list(df[:3].T.to_dict().values())) == 3

    def test_insert_many_from_list_dicts(self):
        mock_list_of_dicts = [
            {'age': 32.0, 'workclass': 'State-gov', 'fnlwgt': 77516.0, 'education': 'Bachelors', 'education_num': 13, 'marital_status': 'Never-married', 'occupation': 'Adm-clerical', 'relationship': 'Not-in-family', 'race': 'White', 'sex': 'Male', 'capital_gain': 2174.0, 'capital_loss': 0, 'hours_per_week': 40, 'native_country': 'United-States', '_class': '<=50K'}, 
            {'age': 50.0, 'workclass': 'Self-emp-not-inc', 'fnlwgt': 83311.0, 'education': 'Bachelors', 'education_num': 13, 'marital_status': 'Married-civ-spouse', 'occupation': 'Exec-managerial', 'relationship': 'Husband', 'race': 'White', 'sex': 'Male', 'capital_gain': 0.0, 'capital_loss': 0, 'hours_per_week': 13, 'native_country': 'United-States', '_class': '<=50K'}, 
            {'age': 38.0, 'workclass': 'Private', 'fnlwgt': 215646.0, 'education': 'HS-grad', 'education_num': 9, 'marital_status': 'Divorced', 'occupation': 'Handlers-cleaners', 'relationship': 'Not-in-family', 'race': 'White', 'sex': 'Male', 'capital_gain': 0.0, 'capital_loss': 0, 'hours_per_week': 40, 'native_country': 'United-States', '_class': '<=50K'}
        ]

        Adult.create_table()
        Adult.insert_many(mock_list_of_dicts).execute()

    def test_algorithm(self):
        adult_extraction_obj = AdultExtraction()
        assert adult_extraction_obj


if __name__ == '__main__':
    unittest.main()