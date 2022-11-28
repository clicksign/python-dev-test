import pandas as pd
import sqlite3
from sqlite3 import Error
from time import sleep

database_name = 'adult'

#Caminho do arquivo .csv a ser inserido
csv_path = r'/home/vegh/Desktop/Estudos/python-dev-test/data/adult.csv'

insert_data_adult = '''
    INSERT OR IGNORE INTO ADULT VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

#criação da conexão com o banco
connection = sqlite3.connect(database_name)
cursor = connection.cursor()
print(f'Conectado a database {database_name}')

#Divide o dataframe transformado no notebook em dataframes menores de 1630 linhas e os agrega em uma lista vazia.
def resize_dataframe(df, row_number):
    dfs = []
    dfs_number = len(df) // row_number + 1
    for value in range(dfs_number):
        dfs.append(df[value * row_number:(value + 1) * row_number])
    return dfs

# A função abaixo utiliza os dataframes menores criados na função resize_dataframe, 
# inserindo cada dataframe num intervalo de 10 segundos através da função executemany do sqlite,
# evitando perda de performance da func execute() que insere apenas uma linha de cada vez.
def insert_into_table(dfs):
    try:
        for df in dfs:
            args = list(tuple(row) for row in df.to_numpy())
            cursor.executemany(insert_data_adult, args)
            connection.commit()
            print('Inserindo dados...')          
            sleep(10)
    except Error as e:
        print(e)
        connection.rollback()


if __name__ == "__main__":
    #Uma execução automática foi configurada via crontab(Linux) utilizando o seguinte cron 5 10 * * * ./insert_data.py
    #Caso haja necessidade de similar, basta executar os seguintes comandos:
    #sudo chmod +x ./path_do_arquivo > crontab -e, 5 10 * * * ./path_do_arquivo > ctrl x > enter > crontab -l(mostra os crons configurados)
    dfs = resize_dataframe(df=pd.read_csv(csv_path, sep=','), row_number=1630)
    insert_into_table(dfs)
    print('Dados inseridos com sucesso!...')
