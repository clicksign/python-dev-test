
import test.output  as to
import data.extract.extract as dee
import sqlite3

# Definindo o nome da tabela
table_name = "adults"

# Creating database adults
conn = sqlite3.connect(f'{table_name}.db')
# Definindo um cursor
cursor = conn.cursor()

#  Criando a tabela de adults
# cursor.execute(f""" CREATE TABLE {table_name}(
#     age INTEGER NOT NULL,            
#     workclass VARCHAR(50) NOT NULL, 
#     fnlwgt INTEGER NOT NULL,
#     education VARCHAR(100) NOT NULL,
#     'education-num' INTEGER NOT NULL,
#     'marital-status' VARCHAR(50) NOT NULL,
#     occupation  VARCHAR(100) NOT NULL,  
#     relationship  VARCHAR(50) NOT NULL,
#     race VARCHAR(50) NOT NULL,
#     sex VARCHAR(10) NOT NULL,
#     'capital-gain'  VARCHAR(500) NOT NULL,
#     'capital-loss'  VARCHAR(500) NOT NULL,
#     'hours-per-week' VARCHAR(500) NOT NULL,
#     'native-country' VARCHAR(100) NOT NULL,
#     class   float NOT NULL
# ); """)
# print('Tabela criada com sucesso.')
# conn.close()

def insert_into(cleaned_data):
    """
    Essa função realiza a inserção dos dados na tabela criada no SQLITE
    Retornando uma mensagem com de sucesso ou de erro
    """
    lista = cleaned_data.values.tolist()
    try:
        cursor.executemany(f""" INSERT INTO {table_name}('age', 'workclass', 'fnlwgt', 'education', 'education-num','marital-status', 'occupation', 'relationship', 'race','sex','capital-gain','capital-loss','hours-per-week','native-country','class')
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) """, lista)
        conn.commit()
        print(f"Dados inseridos com sucesso na tabela {table_name} !")
        conn.close()
    except:
        print(f"Erro na ingestão de dados na tabela {table_name}")
    


def add_columns_name(data):
    """
    Essa função adiciona o nome das colunas no dataframe que contém os dados vindos do source
    """
    data.columns=['age', 'workclass', 'fnlwgt', 'education', 'education-num','marital-status', 'occupation', 'relationship', 'race','sex','capital-gain','capital-loss','hours-per-week','native-country','class']
    return data
def remove_duplicates(df_adult):
    """
    Essa função identifica e remove as linhas duplicadas, criando um dataframe apenas com informações válidas para o negócio
    """
    df = df_adult.drop_duplicates()
    return df

if __name__ == '__main__':
    data = dee.loading_data()
    df_adult = add_columns_name(data)
    cleaned_data = remove_duplicates(df_adult)
    insert_into(cleaned_data)

    # Chama a função que executa um select na tabela adults
    to.return_data(table_name)
    

   
    


 
