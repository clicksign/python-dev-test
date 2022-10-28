from src.config import environment
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base


base_entity = declarative_base() # usado para criacao das entities

class DB:

    '''
    Criacao da engine usando a URI definida no .env
    '''
    
    engine = create_engine(environment.get_item("DATABASE_URI"))

    def get_engine(self):
        return self.engine