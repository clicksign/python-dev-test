import sqlalchemy 
import os

engine = None

def load_sqlite_database():
    global engine

    #create folder if not exists
    if not os.path.exists('database'):
        os.makedirs('database')


    if engine == None:
        engine = sqlalchemy.create_engine('sqlite:///database/db.sqlite', echo=False)

    return engine

def check_sqlite_table(table_name, base_dataset):
    """
    Check if table exists, if not, creates the SQLite table.
    """

    # Check if table already exists in sqlite using sqlalquemy
    if not sqlalchemy.inspect(load_sqlite_database()).has_table(table_name):
        field_and_types = ""

        # Iterate columns names ans types
        for column_name, dtype in zip(base_dataset.columns, base_dataset.dtypes):

            # If dtype is numeric, then add it to the field_and_types
            if dtype == 'float64' or dtype == 'int64':
                field_and_types += f"{column_name} REAL,"
            else: 
                field_and_types += f"{column_name} TEXT,"

        #Open connection, to be able to run sql commands
        conn = load_sqlite_database().connect()
        trans = conn.begin()

        # Create table with infered types by pandas (Because "base_dataset" is a pandas dataframe)
        conn.execute(f"""
            CREATE TABLE {table_name}
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                {field_and_types[:-1]}); 
        """) # [:-1] Remove last comma

        # After commit, the table is created
        trans.commit()

def insert_data(table_name, dataset):
    """
    Insert data into the SQLite table.
    """

    # Validate if table exists
    check_sqlite_table(table_name, dataset)

    dataset.to_sql(
        name=table_name,
        if_exists='append',
        index=False,
        method='multi', # Option to insert multiple rows at once
        chunksize=200,
        con=load_sqlite_database()
    )