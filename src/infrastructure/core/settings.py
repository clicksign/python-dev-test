from enum import Enum
from decouple import config


class Settings(Enum):
    DB_USER = config('DB_USER')
    DB_PASS = config('DB_PASS')
    DB_PORT = config('DB_PORT')
    DB_NAME = config('DB_NAME')
    BASE_PATH = config('BASE_PATH')
    SQLALCHEMY_DATABASE_URI = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@127.0.0.1:{DB_PORT}/{DB_NAME}'
    PATH = f'{BASE_PATH}/python-dev-test'
