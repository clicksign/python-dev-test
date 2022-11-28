import sqlite3
from sqlite3 import Error

database_name = 'adult'

#criação da conexão com o banco
connection = sqlite3.connect(database_name)
cursor = connection.cursor()
print(f'Database {database_name} criada com sucesso!')

#A função abaixo conecta-se ao banco criado e cria uma tabela chamada adult via sql
def create_table(sql_script: str):
    try:
        cursor.execute(sql_script)
        print(f'Tabela criada com sucesso em {database_name}')
        connection.commit()
    except Error as e:
        print(e)

create_table_adult = '''
    CREATE TABLE IF NOT EXISTS ADULT(
        ADULT_ID INTEGER PRIMARY KEY NOT NULL,
        AGE INTEGER,
        WORKCLASS TEXT,
        FNLWGT INTEGER,
        EDUCATION TEXT,
        EDUCATION_NUM INTEGER,
        MARITAL_STATUS TEXT,
        OCCUPATION TEXT,
        RELATIONSHIP TEXT,
        RACE TEXT,
        SEX TEXT,
        CAPITAL_GAIN INTEGER,
        CAPITAL_LOSS INTEGER,
        HOURS_PER_WEEK INTEGER,
        NATIVE_COUNTRY TEXT,
        CLASS TEXT,
        CLASS_TYPE TEXT)'''


if __name__ == "__main__":
    create_table(create_table_adult)


