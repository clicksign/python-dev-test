import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from .variables import VARIABLES


def sqlite_get_dataframe_from(connection: sqlite3.Connection, table: str) -> pd.DataFrame:
    """
    Gets SQLite {table} in {connection}
    @type connection: sqlite3.Connection
    @type table: str
    @rtype: pd.Database
    @param connection: a connection representing the database connection
    @param table: a string representing the table name
    @return: a dataframe representing the table content
    """
    dataframe = pd.read_sql_query(f"SELECT * FROM {table}", connection)
    return dataframe


def sqlite_erase_create_or_update_from(table: str, starting_dataframe=None):
    """
    Erases, create or update SQLite {table} in SQLite_ClickSign.db
    based on {starting_dataframe}
    @type table: str
    @type starting_dataframe: any
    @param table: a string representing the table to erase
    @param starting_dataframe: a optional dataframe representing the starting table content
    """
    if starting_dataframe is not None:
        dataframe = starting_dataframe
    else:
        expected_header = VARIABLES["expected_header"]
        dataframe = pd.DataFrame([], columns=expected_header)
    engine = create_engine(f"sqlite:///SQLite_ClickSign.db")
    dataframe.to_sql(table, engine, if_exists='replace', index=False)


def sqlite_table_exists(connection: sqlite3.Connection, table: str) -> bool:
    """
    Verifies if SQLite {table} in {connection} exists
    @type connection: sqlite3.Connection
    @type table: str
    @rtype: bool
    @param connection: a connection representing the database connection
    @param table: a string representing the table name
    @return: a boolean representing the table existence
    """
    try:
        pd.read_sql_query(f"SELECT * FROM {table}", connection)
    except pd.io.sql.DatabaseError:
        return False
    return True
