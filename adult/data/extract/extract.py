import pandas as pd
def loading_data():
    """
    Essa função realiza a carga de dados da origem para um dataframe utilizando a biblioteca pandas
    """
    data =  pd.read_csv("data/Adult.data", sep=",")
    return data
    
    
