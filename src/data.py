#!/usr/bin/env python
# coding: utf-8

#-----------------------------------------
# Autor: Hamilton Ribeiro
# Data: 04/04/2022
# Desafio: Desenvolvimento de processo ETL
# Release: 1.0
#-----------------------------------------

"""
ETL Documentação
Este processo de ETL tem o intúito de realizar uma Extração -> Transformação -> Carregamento
"""

"""Importação dos módulos utilizados durante o processamento."""
import os
import pandas as pd
import sqlite3


# [Inicio do processo de Extração]

"""Definindo da função que extrairá os dados dos datasets."""
def extract_process(data_path):
    # Com o método listdir(), será listado os arquivos contidos no diretório
    staging_files = os.listdir(data_path)
    
    # Var utilizada para limitar a quantidade de linhas em cada arquivo
    count_lines = 815

    for file in staging_files:
        """Leitura do datasets Adult.test utilizando o método starswith()."""
        if file.startswith('Adult.test'):
  
            # Exibindo o nome do arquivo que está sendo lido
            print('Loading File {}...'.format(file))

            # Apendando na lista as linhas lidas do arquivo
            df_test = pd.read_csv(os.path.join(data_path, file),
                                  sep=',',
                                  skiprows=1,
                                  header=None,
                                  nrows=count_lines)

        """Leitura do datasets Adult.data utilizando o método starswith()."""
        if file.startswith('Adult.data'):

            # Exibindo o nome do arquivo que está sendo lido
            print('Loading File {}...'.format(file))

            # Apendando na lista as linhas lidas do arquivo
            df_data = pd.read_csv(os.path.join(data_path, file),
                                  sep=',', 
                                  header=None,
                                  nrows=count_lines)

    """Concatenando as duas listas que foram geradas."""
    df_principal = pd.concat([df_data, df_test])

    """Gerando um novo arquivo .csv consolidado na pasta staging."""
    df_principal.to_csv('staging/Adult_staging.csv', index=False, header=None)

# [Fim do processo de Extração]

# [Inicio do processo de Transformação]

"""Definindo da função de transformação dos dados."""
def transform_process(staging_path):
    # Preaparando os dados
    # Substituindo caracteres indesejados por NaN
    valores_ausentes = ['?', ' ?', ' ']

    # Criando uma lista para nomear as colunas
    column_names = [
        'age', 
        'workclass', 
        'fnlwgt', 
        'education', 
        'education_num', 
        'marital_status', 
        'occupation', 
        'relationship', 
        'race',
        'sex',
        'capital_gain',
        'capital_loss',
        'hours_per_week',
        'native_country',
        'isclass_gt_eq_50K'
    ]
    
    # Acessando a fonte de dados armazenada na pasta staging
    processing_files = os.listdir(staging_path)
    
    # Processamento do arquivo consolidado
    for file in processing_files:
        if file.startswith('Adult'):
            df = pd.read_csv(os.path.join(staging_path, file),
                             sep=",",
                             header=None,
                             names=column_names,
                             na_values = valores_ausentes)

    
    print('Geração do dataframe que passará pela transformação...')
    
    # Transformação da coluna isclass_gt_eq_50K
    df['isclass_gt_eq_50K'] = \
    df['isclass_gt_eq_50K'].replace(
                                [' >50K', ' >50K.', ' <=50K', ' <=50K.'], 
                                [1, 1, 0, 0])
    
    """Limpeza dos dados."""
    # Remoção as linhas com NaN
    df = df.dropna()

    # Remoção as linhas duplicadas
    df = df.drop_duplicates()
    
    print('Dados preparados para serem persistidos no banco de dados...')
    
    return df

"""Definindo da função que iniciará o processo de extração e transformação."""
def etl_process():
    #Acessando as fontes de dados
    data_path = './data/'
    staging_path = './staging/'
    
    # Chamando a função de extração passando o caminho da origem
    extract_process(data_path)
    
    # Chamando a função de extração passando o caminho da destino
    df = transform_process(staging_path)
    
    return df

# [Fim do processo de Transformação]

# [Início do processo de Carregamento]

"""
### Interagindo com o banco de dados
O SQLite foi o banco de dados utilizado para compor este processo
que armazenará os dados extraídos e transformados.
"""

"""Definindo a função para criação da tabela no banco de dados."""
def create_table(curr):
    stmt_create_table = ("""CREATE TABLE data (
        age INTEGER
        ,workclass TEXT
        ,fnlwgt REAL
        ,education TEXT
        ,education_num REAL
        ,marital_status TEXT
        ,occupation TEXT
        ,relationship TEXT
        ,race TEXT
        ,sex TEXT
        ,capital_gain REAL
        ,capital_loss REAL
        ,hours_per_week INTEGER
        ,native_country TEXT
        ,isclass_gt_eq_50K TEXT
        );""")
    curr.execute(stmt_create_table)
    
# Definindo a função de inserção dos dados lidos para tabela
# passando as variáveis do cursor e colunas como parâmetro
def insert_into_table(curr, age, workclass, fnlwgt, education, education_num,
                      marital_status, occupation, relationship, race, sex,
                      capital_gain, capital_loss, hours_per_week,
                      native_country, isclass_gt_eq_50K):
    
    # variável que armazenará o script de inserção
    stmt_insert_into_table = ("""INSERT INTO data (
        age
        ,workclass
        ,fnlwgt
        ,education
        ,education_num
        ,marital_status
        ,occupation
        ,relationship
        ,race
        ,sex
        ,capital_gain
        ,capital_loss
        ,hours_per_week
        ,native_country
        ,isclass_gt_eq_50K)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""")
    
    # variável que armazenara os dados lidos
    row_to_insert = (age, workclass, fnlwgt, education, education_num,
                     marital_status, occupation, relationship, race, sex,
                     capital_gain, capital_loss, hours_per_week,
                     native_country, isclass_gt_eq_50K)
    
    # Execução do comando que irá persistir os dados no banco de dados
    curr.execute(stmt_insert_into_table, row_to_insert)

"""Definindo a função de iteração no dataframe para leitura dos dados."""
def load_process(curr, df):
    for i, row in df.iterrows():
        insert_into_table(curr, row['age'], row['workclass'], row['fnlwgt']
                          , row['education'], row['education_num']
                          , row['marital_status'], row['occupation']
                          , row['relationship'], row['race'], row['sex']
                          , row['capital_gain'], row['capital_loss']
                          , row['hours_per_week'], row['native_country']
                          , row['isclass_gt_eq_50K'])

"""
Definindo a função que executará deleção dos dados da tabela e
limpeza de espaços não utilizados.
"""
def truncate_table(curr):
    stmt_truncate_table = ("""DELETE FROM data;""")    
    curr.execute(stmt_truncate_table)
    
    #stmt_clear_space = ("""VACUUM;""")
    #curr.execute(stmt_clear_space)

"""Definindo função que executará script para apagar a tabela."""
def drop_table(curr):
    stmt_drop_table = ("""DROP TABLE data;""")
    curr.execute(stmt_drop_table)

# [Fim do processo de Carregamento]

"""Main."""
if __name__ =='__main__':
    # Executando o programa de ETL
    df = etl_process()

    """Estabelecendo uma conexão para db."""
    # script para criar o banco de dados
    db = sqlite3.connect('./db/adult.db')

    # Criando o cursor para execução dos scripts
    curr = db.cursor()

    # Criando a tabela
    create_table(curr)
    db.commit()
    print('Tabela criada com sucesso.')

    # Inserindo os dados na tabela    
    load_process(curr, df)
    db.commit()
    print('Dados gravados com sucesso.')


    """Caso seja necessario, eliminar a tabela."""
    # drop_table(curr)
    # print('Tabela apagada com sucesso!')
    # db.commit()

    """Caso seja necessario, eliminar os dados da tabela."""
    # truncate_table(curr)
    # db.commit()

    """Visualizar dados da tabela."""
    curr.execute("SELECT * FROM data LIMIT(10)")
    print(curr.fetchall())


"""Config arquivo de crontab para rodar o arquivo a cada 10 minutos."""
# # crontab -e
# 
# # 10 * * * * /usr/bin/python ./processo_etl.py
