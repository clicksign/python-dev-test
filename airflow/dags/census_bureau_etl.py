import os
import pickle
import sqlalchemy
import pandas as pd


class Process:
    def __init__(self, index, length) -> None:
        self.index = index
        self.length = length

    def process(self) -> None:
        self.index += self.length

    def get_length(self) -> int:
        return self.length

    def get_index(self) -> int:
        return self.index


def get_db():
    DIR = os.path.abspath(os.path.dirname(__file__))
    DB_LOCATION = f"sqlite:///{DIR}/db/census_bureau.sqlite"

    if not os.path.exists(f"{DIR}/db/"):
        os.makedirs(f"{DIR}/db/")

    return sqlalchemy.create_engine(DB_LOCATION)


def transform(df):
    NUMERIC_COLS = [
        "age",
        "fnlwgt",
        "education_num",
        "capital_gain",
        "capital_loss",
        "hours_per_week",
    ]

    df[NUMERIC_COLS] = df[NUMERIC_COLS].apply(pd.to_numeric, errors="coerce")

    for column in df.columns[df.isnull().any()]:
        df[column] = df[column].fillna(
            df.groupby("class")[column].transform("mean")
        )

    df = df.drop(["relationship"], axis=1)

    return df


def extract_census_bureau():
    cols = [
        "age",
        "workclass",
        "fnlwgt",
        "education",
        "education-num",
        "marital-status",
        "occupation",
        "relationship",
        "race",
        "sex",
        "capital-gain",
        "capital-loss",
        "hours-per-week",
        "native-country",
        "class",
    ]

    cols = [attribute.replace("-", "_") for attribute in cols]

    DIR = os.path.abspath(os.path.dirname(__file__))

    if not os.path.exists(f"{DIR}/utils/"):
        os.makedirs(f"{DIR}/utils/")

    if not os.path.exists(f"{DIR}/utils/process.pkl"):
        with open(f"{DIR}/utils/process.pkl", "wb") as f:
            process = Process(0, 815)
            pickle.dump(process, f)

    with open(f"{DIR}/utils/process.pkl", "rb") as f:
        process = pickle.load(f)

    length = process.get_length()
    index = process.get_index()

    try:
        data = pd.read_csv(
            f"{DIR}/data/Adult.data",
            delimiter=",",
            skiprows=index,
            header=None,
            names=cols,
            nrows=length,
        )
        data.to_csv("/tmp/data_extract.csv", index=False)
    except Exception:
        print("End data load!")

    try:
        test = pd.read_csv(
            f"{DIR}/data/Adult.test",
            delimiter=",",
            skiprows=1 + index,
            header=None,
            names=cols,
            nrows=length,
        )
        test.to_csv("/tmp/test_extract.csv", index=False)
    except Exception:
        print("End test load!")

    process.process()

    with open(f"{DIR}/utils/process.pkl", "wb") as f:
        pickle.dump(process, f)


def transform_census_bureau():
    try:
        data = pd.read_csv("/tmp/data_extract.csv")
        data = transform(data)
        data.to_csv("/tmp/data_transform.csv", index=False)
    except Exception:
        print("End data load!")

    try:
        test = pd.read_csv("/tmp/test_extract.csv")
        test = transform(test)
        test.to_csv("/tmp/test_transform.csv", index=False)
    except Exception:
        print("End test load!")


def load_census_bureau():
    engine = get_db()

    try:
        data = pd.read_csv("/tmp/data_transform.csv")
        data.to_sql(
            "census_bureau_data", engine, index=False, if_exists="append"
        )
    except Exception:
        print("Data already loaded!")

    try:
        test = pd.read_csv("/tmp/test_transform.csv")
        test.to_sql(
            "census_bureau_test", engine, index=False, if_exists="append"
        )
    except Exception:
        print("Test already loaded!")
