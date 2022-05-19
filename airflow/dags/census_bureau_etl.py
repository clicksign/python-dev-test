import os
import pickle
import sqlalchemy
import pandas as pd


class Process:
    def __init__(self, index, length) -> None:
        self.index = index
        self.length = length
        self.data = True

    def process(self) -> None:
        self.index += self.length

    def get_length(self) -> int:
        return self.length

    def get_index(self) -> int:
        return self.index

    def get_data(self) -> bool:
        return self.data

    def set_data(self, data) -> None:
        self.data = data


def get_db():
    DIR = os.path.abspath(os.path.dirname(__file__))
    DB_LOCATION = f"sqlite:///{DIR}/db/census_bureau.sqlite"

    if not os.path.exists(f"{DIR}/db/"):
        os.makedirs(f"{DIR}/db/")

    return sqlalchemy.create_engine(DB_LOCATION)


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
            process = Process(0, 1630)
            pickle.dump(process, f)

    with open(f"{DIR}/utils/process.pkl", "rb") as f:
        process = pickle.load(f)

    length = process.get_length()
    index = process.get_index()

    if process.get_data():
        data = pd.read_csv(
            f"{DIR}/data/Adult.data",
            delimiter=",",
            skiprows=index,
            header=None,
            names=cols,
            nrows=length,
        )

        data.to_csv("/tmp/data_extract.csv", index=False)

        if len(data) < length:
            process.set_data(False)
    else:
        df_test = pd.read_csv(
            f"{DIR}/data/Adult.test",
            delimiter=",",
            skiprows=1 + index,
            header=None,
            names=cols,
            nrows=length,
        )

        df_test.to_csv("/tmp/data_extract.csv", index=False)

    process.process()

    with open(f"{DIR}/utils/process.pkl", "wb") as f:
        pickle.dump(process, f)


def transform_census_bureau():
    NUMERIC_COLS = [
        "age",
        "fnlwgt",
        "education_num",
        "capital_gain",
        "capital_loss",
        "hours_per_week",
    ]
    try:
        df = pd.read_csv("/tmp/data_extract.csv")

        df[NUMERIC_COLS] = df[NUMERIC_COLS].apply(
            pd.to_numeric, errors="coerce"
        )

        for column in df.columns[df.isnull().any()]:
            df[column] = df[column].fillna(
                df.groupby("class")[column].transform("mean")
            )

        df = df.drop(["relationship"], axis=1)

        df.to_csv("/tmp/data_transform.csv", index=False)
    except Exception:
        print("End data load!")


def load_census_bureau():
    engine = get_db()

    try:
        df = pd.read_csv("/tmp/data_transform.csv")
        df.to_sql("census_bureau", engine, index=False, if_exists="append")
    except Exception:
        print("Data already loaded!")
