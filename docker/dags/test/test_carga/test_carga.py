import sys
import os       
import inspect         
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(os.path.dirname(currentdir)                )
sys.path.append(parentdir)
                
import scripts.carga as carga

import pandas as pd
import pytest

def test_remove_espacos():
    
    df = pd.DataFrame(
        {
            "age": [10, 20, 33],
            "workclass": ['Private class', 'Lower class', 'Upper Class'],
        }
    )
    carga._remove_espacos(df, 'workclass')
    print(df['workclass'][0])
    assert df['workclass'][0] == 'Private class'