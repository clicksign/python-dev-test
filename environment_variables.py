import os

DB_USER = os.getenv('DB_USER') or 'user'
DB_PASS = os.getenv('DB_PASS') or 'passwd'
DB_HOST = os.getenv('DB_HOST') or 'host.com'
DB_PORT = os.getenv('DB_PORT') or 5432
DB_NAME = os.getenv('DB_NAME') or 'db_name'
DB_CONN = f'postgresql://{DB_USER}:@{DB_HOST}:{DB_PORT}/{DB_NAME}'
