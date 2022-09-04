import sys
sys.path.append('../')

from etl_clicksign.models import Adult
from etl_clicksign.__init__ import __version__

def test_version():
    assert __version__ == '0.1.0'

def test_create_adult_table():
    Adult.create_table()
    assert Adult

def test_insert_adult_table():
    mock_adult_insert = {
        "age" : 39, 
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

test_version()
test_create_adult_table()
test_insert_adult_table()
