from script.transform import handle_missing_data
import pandas as pd

def test_must_handle_int64_missing_data():

    # ------> Arrange    
    df = pd.DataFrame({ # Create a dataframe with missing values
        'col1': [1.64, 2.45, 0.34853, 345.87, 456.18, 0.0000002448], 
        'col2': [464, None, 2646, 3487, 46587, 2185],
        'col3': ['dog', 'dog', 'cat', 'cat', 'fish', 'bird'],
        'col4': [23, 1455665, None, 45623, 24234, 0],
        'col5': [56.4, 3.11, 6.34, 0.24, 4.687, 1564564.36484]
    }) 

    # ------> Act
    handle_missing_data(df)

    # ------> Assert
    assert df.isna().sum().sum() == 0


def test_must_handle_float_missing_data():

    # ------> Arrange    
    df = pd.DataFrame({ # Create a dataframe with missing values
        'col1': [1.64, 2.45, 0.34853, None, None, None], 
        'col2': [464, 0, 2646, 3487, 46587, 2185],
        'col3': ['dog', 'dog', 'cat', 'cat', 'fish', 'bird'],
        'col4': [23, 1455665, 456, 45623, 24234, 0],
        'col5': [56.4, None, 6.34, None, 4.687, None]
    }) 

    # ------> Act
    handle_missing_data(df)

    # ------> Assert
    assert df.isna().sum().sum() == 0


def test_must_handle_categoric_missing_data():

    # ------> Arrange    
    df = pd.DataFrame({ # Create a dataframe with missing values
        'col1': [1.64, 2.45, 0.34853, 345.87, 456.18, 0.0000002448],
        'col2': [464, 0, 2646, 3487, 46587, 2185],
        'col3': ['dog', 'dog', 'cat', 'cat', None, 'bird'],
        'col4': [23, 1455665, 456, 45623, 24234, 0],
        'col5': [56.4, 3.11, 6.34, 0.24, 4.687, 1564564.36484]
    }) 

    # ------> Act
    handle_missing_data(df)

    # ------> Assert
    assert df.isna().sum().sum() == 0


def test_must_handle_all_missing_data():

    # ------> Arrange    
    df = pd.DataFrame({ # Create a dataframe with missing values
        'col1': [1.64, 2.45, 0.34853, 345.87, 456.18, 0.0000002448],
        'col2': [464, None, 2646, 3487, 46587, 2185],
        'col3': ['dog', 'dog', 'cat', 'cat', None, 'bird'],
        'col4': [23, 1455665, None, 45623, 24234, 0],
        'col5': [56.4, None, 6.34, None, 4.687, None]
    }) 

    # ------> Act
    handle_missing_data(df)

    # ------> Assert
    assert df.isna().sum().sum() == 0