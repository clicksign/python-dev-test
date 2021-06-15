import sqlite3
import pandas
from sqlalchemy import create_engine
from .variables import VARIABLES


def sqlite_create_table_on(dataframe: pandas.DataFrame, table: str):
    """
    Creates SQLite {table} in SQLite_ClickSign.db based on {dataframe}
    @type dataframe: pandas.Dataframe
    @type table: str
    @param dataframe: a dataframe representing the table to be created
    @param table: a string representing the table name
    """
    engine = create_engine(f"sqlite:///SQLite_ClickSign.db")
    dataframe.to_sql(table, engine, if_exists='replace', index=False)


def sqlite_get_dataframe_from(connection: sqlite3.Connection, table: str) -> pandas.DataFrame:
    """
    Gets SQLite {table} in {connection}
    @type connection: sqlite3.Connection
    @type table: str
    @rtype: pandas.Database
    @param connection: a connection representing the database connection
    @param table: a string representing the table name
    @return: a dataframe representing the table content
    """
    dataframe = pandas.read_sql_query(f"SELECT * FROM {table}", connection)
    return dataframe


def sqlite_erase_from(table: str, starting_dataframe=None):
    """
    Erases SQLite {table} in SQLite_ClickSign.db
    @type table: str
    @type starting_dataframe: any
    @param table: a string representing the table to erase
    @param starting_dataframe: a optional dataframe representing the starting table content
    """
    if starting_dataframe is not None:
        dataframe = starting_dataframe
    else:
        expected_header = VARIABLES["expected_header"]
        dataframe = pandas.DataFrame([], columns=expected_header)
    engine = create_engine(f"sqlite:///SQLite_ClickSign.db")
    dataframe.to_sql(table, engine, if_exists='replace', index=False)


def sqlite_table_exists(connection: sqlite3.Connection, table: str) -> bool:
    try:
        pandas.read_sql_query(f"SELECT * FROM {table}", connection)
    except pandas.io.sql.DatabaseError:
        return False
    return True
