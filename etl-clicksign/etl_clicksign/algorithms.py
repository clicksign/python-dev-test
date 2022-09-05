#!/usr/bin/python
import pandas as pd
from dataclasses import dataclass, field
from typing import List

@dataclass
class AdultExtraction:
    global_path = '/home/aantunesnds/Desktop/python-dev-test/' 
    df : pd.DataFrame = field(init=False)

    def __post_init__(self):
        self.create_df()
        self.transform_fields()

    def create_df(self):
        columns_df = ["age", "workclass", "fnlwgt", "education", "education_num", "marital_status", "occupation",\
           "relationship", "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country",\
           "_class"
        ]

        self.df = pd.read_csv(
            f'{self.global_path}data/Adult.data', 
            skipinitialspace = True, 
            delimiter = ',', 
            names=columns_df
        )

    def transform_fields(self):
        self.df["age"] = pd.to_numeric(self.df["age"], errors='coerce')
        self.df["fnlwgt"] = pd.to_numeric(self.df["fnlwgt"], errors='coerce')
        self.df["education_num"] = pd.to_numeric(self.df["education_num"], errors='coerce')
        self.df["capital_gain"] = pd.to_numeric(self.df["capital_gain"], errors='coerce')
        self.df["capital_loss"] = pd.to_numeric(self.df["capital_loss"], errors='coerce')
        self.df["hours_per_week"] = pd.to_numeric(self.df["hours_per_week"], errors='coerce')
