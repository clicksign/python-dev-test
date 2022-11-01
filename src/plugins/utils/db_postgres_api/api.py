import psycopg2 as pg
from psycopg2 import extras
from pandas.core.frame import DataFrame
import logging
import time
import numpy as np

class DBPostgres:
    """
    DBPostgres: class created to make operations in a db postgres with
    more facility.
    args:
    :param host: string type        - the host on the postgres is located
    :param databse: string type     - the database to operate
    :user: string type              - the user of postgres
    :password:string type           - the password of the user
    """
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        #self.password = password
        
        self.__conn = self.__connect(password)
        
    def __connect(self, password):
        return pg.connect(host = self.host,
                          database = self.database,
                          user = self.user,
                          password = password) 

    def execute(self, command):
        """Method execute
        Args:
            command (string): command sql to operate in db postgres
        Returns:
           Boolean
        """
        try:
            with self.__conn.cursor() as cursor:
                cursor.execute(command)
            self.__conn.commit()
            logging.info("Ok - command executed with sucess")
            return True
        except Exception as e:
            self.__conn.rollback()
            logging.exception("Exception occurred")
            raise
        
    def truncate(self, table, schema = "public"):
        """Method truncate
        Args:
            table (string): the table name
            schema (string default "public"): the schema name, public default
        Returns:
            boolean
        """
        self.execute(f"TRUNCATE {schema}.{table} CASCADE")
            
    def get_results_query(self, query, interval_register = 500):
        """Method get_results_query
        Method used to return data from a postgres database in dictionary form
        Args:
            query (string): the query to execute for returning data
            interval_register (int): interval to returnind data, default: 150
        Returns:
            dict type
        """
        with self.__conn.cursor(cursor_factory=extras.DictCursor) as cursor:
            cursor.execute(query)
            colunas = [x[0] for x in cursor.description]
            
            results = []
            for result in cursor.fetchmany(interval_register):
                results.extend(result)
        return {"cols":colunas, 'data':results}
    
    def insert(self, 
               columns:list, 
               data:list, 
               table:str, 
               schema:str = "public", 
               range_data:int = 1000, 
               step_time:int = 10):
        print("Insert process initiated")
        """Method insert
        Args:
            columns (List[str]): set of columns
            data (List): set of data
            table (string): the table name
            schema (string default "public"): the schema name, public default
            range_data (int default 1000): interval of rows number to insert
            step_time (int default 10): interval of time to insert
        Returns:
            None
        """
        columns = [('"'+column+'"') for column in columns]
        insert = f'''
            INSERT INTO {schema}.{table} ({', '.join(columns)}) VALUES ({", ".join(["%s"]*len(columns))})
        '''
        a = 0
        b = range_data
        try:
            len_data = len(data)
            while True: 
                with self.__conn.cursor() as cursor:
                    cursor.executemany(insert, [tuple(row) 
                                                for row in data[a:b]])
                self.__conn.commit()
                logging.info(f"Rows {a+1}:{b} insert with success")
                a = b
                b += range_data  
                if b >= len_data:
                    b = len_data - 1
                    with self.__conn.cursor() as cursor:
                        cursor.executemany(insert, [tuple(row) 
                                                    for row in data[a:]])
                    self.__conn.commit()         
                    logging.info(f"Rows {a+1}:{len_data} insert with success")   
                    break
                time.sleep(step_time)  
        except Exception as e:
            self.__conn.rollback()
            logging.error("Exception occurred")
            raise
                
    def upsert(self, 
               columns:list, 
               data:list, 
               table:str, 
               schema:str = "public", 
               range_data:int = 1000, 
               step_time:int = 10):
        table_info = self.table_information(table, schema)
        print(table_info)
        insert = f'''
            INSERT INTO {schema}.{table} ({", ".join(tuple(columns))}) VALUES ({", ".join(['%s']*len(columns))})
            on conflict ({", ".join(tuple(columns))}) do update
            set {",".join([(col+"=EXCLUDED."+col) for table, col in table_info])} 
        '''
        a = 0
        b = range_data
        try:
            len_df = len(data)
            while True:
                if b >= len_df:
                    with self.__conn.cursor() as cursor:
                        cursor.executemany(insert, [tuple(row) for row in data[a:]])
                    self.__conn.commit()         
                    print(f"Rows {a+1}:{len_df} insert with success")  
                    break
                with self.__conn.cursor() as cursor:
                    cursor.executemany(insert, [tuple(row) 
                                                for row in data[a:b]])
                self.__conn.commit()
                print(f"Rows {a+1}:{b} insert with success")
                
                a = b
                b += range_data 
                time.sleep(step_time)
        except Exception as e: 
            self.__conn.rollback()
            logging.exception("Exception occurred")
        
    def all_tables_from_schema(self, schema:str):
        return self.get_results_query("SELECT table_name " + 
                                      "FROM information_schema.tables " +
                                      f"WHERE table_schema = '{schema}'")
    
    def table_information(self, table:str, schema:str = "public"):
        return self.get_results_query("SELECT table_name, column_name" + 
                                      "FROM information_schema.columns " +
                                      f"WHERE table_schema = '{schema}' and table_name = {table}")
    
    def close(self):
        self.__conn.close()
       
class DBPostgresPandas(DBPostgres):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    def insert_df_pandas(self, 
                         df:DataFrame, 
                         table:str, schema:str = 'public', 
                         range_data:int = 1000,
                         step_time:int = 10):
        """Method insert_df_pandas
        This method insert a DataFrame Object Pandas into a table, observated some rules:
         - The name cols must to be the same of the columns table
         - The data type must to be like the data type of the table
        Args:
            df (pandas.core.frame.Dataframe: DataFrame object pandas to insert into a table
            table (string): the table name
            schema (string default "public"): the schema name, public default
            range_data (int default 1000): interval of rows number to insert
            step_time (int default 10): interval of time to insert
        Returns:
            None
        """
        columns = list(df.columns)
        self.insert(columns = columns, 
                    data = df.values.tolist(), 
                    table = table, schema = schema, 
                    range_data=range_data,
                    step_time = step_time) 
          
    def create_table_from_df_pandas(self, 
                                    df:DataFrame, 
                                    table:str, 
                                    schema:str="public", 
                                    range_data:int = 1000,
                                    PK:str = None, 
                                    step_time:int = 10):
        """Method created_table_from_df_pandas
        This method created_table_from_df_pandas create a table with the same parameters
        of DataFrame and insert the data into a table.
        Args:
            df (pandas.core.frame.Dataframe: DataFrame object pandas to insert into a table
            table (string): the table name
            schema (string default "public"): the schema name, public default
            range_data (int default 1000): interval of rows number to insert
            PK(str default None): Primaty key if set
            step_time (int default 10): interval of time to insert
        Returns:
            None
        """
        def columns_type(columns, dtype):
            if str(dtype).__contains__('int'):
                return f"\n         {columns} integer"
            elif str(dtype).__contains__('float'):
                return f"\n         {columns} decimal(10,4)"
            elif str(dtype).__contains__('object'):
                return f"\n         {columns} text"
            elif str(dtype).__contains__('date'):
                return f"\n         {columns} timestamp" 
                
        if PK:
            pk_script = "id_table serial primary key,\n"
        else:
            pk_script = ""
        
        table_script = (f"CREATE TABLE IF NOT EXISTS {schema}.{table}(" +
                        f"{pk_script}")

        df_info = list(zip(df.columns, df.dtypes))
        a = 0       
        for column, dtype in df_info:
            if a < (df.shape[1] - 1):
                table_script = table_script + columns_type(column, str(dtype)) + ','
            else:
                table_script = table_script + columns_type(column, str(dtype))
            a += 1       
                
        print(f'{table_script}')
        self.execute(table_script)
        self.insert_df_pandas(df = df, 
                              table = table, 
                              range_data=range_data, 
                              step_time = step_time)