from utils.db_postgres_api import DBPostgresPandas as db
from utils.get_data import get_data as gd
from dataprocessing.main import main
from airflow.hooks.base import BaseHook as bh
from airflow.models.baseoperator import BaseOperator
import pandas as pd
import json, re, logging

class DataToPostgresOperator(BaseOperator):
    methods = {
        "execute",
        "truncate",
        "insert",
        "insert_df_pandas"
    }
    def __init__(self, 
                 task_id:str, 
                 method:str, 
                 conn_id:str,
                 path_file:str = None,
                 cols_type:dict = None, 
                 table_name:str = None, 
                 range_data:str = None, 
                 step_time:str = None,
                 delimiter:str = ",",
                 encoding:str = None,
                 **kwargs):
        super().__init__(task_id = task_id, **kwargs)
        
        self.table_name = table_name
        self.range_data = range_data
        self.method = method
        self.conn_id = conn_id
        self.step_time = step_time
        self.cols_type = cols_type
        self.path_file = path_file
        self.delimiter = delimiter
        self.encoding = encoding
        #self.query = query
        #self.columns = columns
        
        self.maneger = self.__connect()
        
        if method not in self.methods:
            raise Exception(f"The method {method} not implemented")
    
    def __connect(self):
        cred = bh.get_connection(self.conn_id)
        #cred = bh.get_connection("postgres_id")
        login = cred.login
        password = cred.password
        database = json.loads(cred.extra)['database']
        host = cred.host
                
        return db(user = login, password = password, database = database, host = host)
        
    def execute(self, context):
        logging.info(f"Method: {self.method}")
        if self.method == "truncate":
            eval(f"self.maneger.truncate(self.table_name)")
            
        else:
            if self.path_file is None:
                raise Exception("File path is None. Please, put a value in path_file")
            # Running methoed 
            elif self.method == "execute":
                logging.info(f"Running query from path {self.path_file}")
                query = self.read_file_sql()
                eval(f"self.maneger.execute(query)")    
                
            elif self.method == "insert_df_pandas":
                df = main(self.path_file, self.cols_type)
                logging.info(f"Running method {self.method}")
                eval(f"self.maneger.{self.method}(df = df," +
                    "table = self.table_name,"+
                    "range_data = self.range_data,"+
                    "step_time = self.step_time)")
                
            elif self.method == "insert":
                data = self.read_file_data()
                columns = self.cols_type.keys()
                logging.info(f"Running method {self.method}")
                eval(f"self.maneger.{self.method}(data = data," +
                    "table = self.table_name,"+
                    "columns = columns, " +
                    "range_data = self.range_data,"+
                    "step_time = self.step_time)")
        self.maneger.close()
        
    # Reading a query string into a file .sql
    def read_file_sql(self):
        with open(self.path_file, "r", encoding="utf-8") as q:
            query = q.read()
        return query
    # Reading a data set from path_file
    def read_file_data(self):
        return gd(self.path_file)
