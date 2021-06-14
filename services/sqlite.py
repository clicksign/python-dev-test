import sqlite3

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


def get_dataframe_from(sqlite_connection: sqlite3.Connection, table: str) -> pandas.DataFrame:
    f"""
    Gets SQLite {table} in {sqlite_connection}
    @type sqlite_connection: sqlite3.Connection
    @type table: str
    @rtype: pandas.Database
    @param sqlite_connection: a connection representing the database connection
    @param table: a string representing the table name
    @return: a dataframe representing the table content
    """
    dataframe = pandas.read_sql_query(f"SELECT * FROM {table}", sqlite_connection)
    return dataframe


def sqlite_erase_from(sqlite_connection: sqlite3.Connection, table: str):
    f"""
    Erases SQLite {table} in {sqlite_connection}
    @type sqlite_connection: sqlite3.Connection
    @type table: str
    @param sqlite_connection: a connection representing the database connection
    @param table: a string representing the table to erase
    """
    dataframe = pandas.DataFrame()
    engine = create_engine(f"sqlite:///SQLite_ClickSign.db")
    dataframe.to_sql(table, engine, if_exists='replace', index=False)


connection = sqlite3.connect("SQLite_ClickSign.db")
create_sqlite_table_from(connection, "teste")
