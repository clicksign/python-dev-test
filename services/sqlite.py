import pandas
from sqlalchemy import create_engine


def create_sqlite_table_from(dataframe: pandas.DataFrame, table: str):
    """
    Creates SQLite {table} in SQLite_ClickSign.db based on {dataframe}
    @type dataframe: pandas.Dataframe
    @type table: str
    @param dataframe: a dataframe representing the table to be created
    @param table: a string representing the table name
    """
    engine = create_engine(f"sqlite:///SQLite_ClickSign.db")
    dataframe.to_sql(table, engine, if_exists='replace', index=False)
