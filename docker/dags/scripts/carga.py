# %%
import pandas as pd
import os
import csv
from airflow.hooks.postgres_hook import PostgresHook 


if 'DATA_FILES' in os.environ:
    DATA_PATH =  os.environ['DATA_FILES']
else:
    DATA_PATH = ''

colunas = [
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
    'class',
]

LIST_WORKCLASS = [
    'Private',
    'Self-emp-not-inc',
    'Self-emp-inc',
    'Federal-gov',
    'Local-gov',
    'State-gov',
    'Without-pay',
    'Never-worked'
]
LIST_EDUCATION = [
    'Bachelors',
    'Some-college',
    '11th',
    'HS-grad',
    'Prof-school',
    'Assoc-acdm',
    'Assoc-voc',
    '9th',
    '7th-8th',
    '12th',
    'Masters',
    '1st-4th',
    '10th',
    'Doctorate',
    '5th-6th',
    'Preschool'
]

LIST_MARITAL_STATUS = [
    'Married-civ-spouse', 
    'Divorced', 
    'Never-married', 
    'Separated', 
    'Widowed', 
    'Married-spouse-absent', 
    'Married-AF-spouse'
]

LIST_NATIVE_COUNTRIES = [
    'United-States', 
    'Cambodia', 
    'England', 
    'Puerto-Rico', 
    'Canada', 
    'Germany', 
    'Outlying-US(Guam-USVI-etc)', 
    'India', 
    'Japan', 
    'Greece', 
    'South', 
    'China',
    'Cuba',
    'Iran',
    'Honduras',
    'Philippines',
    'Italy',
    'Poland',
    'Jamaica',
    'Vietnam',
    'Mexico',
    'Portugal',
    'Ireland',
    'France',
    'Dominican-Republic',
    'Laos',
    'Ecuador',
    'Taiwan',
    'Haiti',
    'Columbia',
    'Hungary',
    'Guatemala',
    'Nicaragua',
    'Scotland',
    'Thailand',
    'Yugoslavia',
    'El-Salvador',
    'Trinadad&Tobago',
    'Peru',
    'Hong',
    'Holand-Netherlands'
]
    
LIST_OCCUPATION = [
      'Tech-support',
      'Craft-repair',
      'Other-service',
      'Sales',
      'Exec-managerial',
      'Prof-specialty',
      'Handlers-cleaners',
      'Machine-op-inspct',
      'Adm-clerical',
      'Farming-fishing',
      'Transport-moving',
      'Priv-house-serv',
      'Protective-serv',
      'Armed-Forces',
] 

LIST_RELATIONSHIP = [ 
    'Wife',
    'Own-child',
    'Husband',
    'Not-in-family',
    'Other-relative',
    'Unmarried'
]

LIST_RACE = [
    'White',
    'Asian-Pac-Islander',
    'Amer-Indian-Eskimo',
    'Other',
    'Black'
]

LIST_SEX  = [
    'Female',
    'Male',
]

LIST_CLASS = [
    '>50K', 
    '<=50K'
]

def _ler_arquivo_entrada(filename):
    ''' Recebe o parh de um arquivo de entrada e retorna um Dataframe'''
    file = open(filename, 'r')
    linhas_skip = 0
    while True:
        line = file.readline().strip()
        if line[:1] == '|':
            linhas_skip = 1
            break
        else:
            break
    df = pd.read_csv(filename, skiprows=linhas_skip, sep=',')
    df.columns = colunas
    return df

def _busca_arquivos(diretorio):
    ''' Recebe um diretório de entrada e realiza a leitura de todos os arquivos iniciados com Adult e que não sejam finalizados por .prc
        Retorna um dataframe total e uma lista com o nome dos arquivos'''
    arquivos = []
    df_total = pd.DataFrame()
    for file in os.listdir(diretorio):
        if file.startswith('Adult') and not file.endswith('.prc'):
            df = _limpeza(_ler_arquivo_entrada(os.path.join(diretorio, file)))
            
            df_total = pd.concat([df_total, df])
            arquivos.append(os.path.join(diretorio, file))
            
    return df_total, arquivos

def _remove_espacos(df, campo):
    ''' Recebe um Dataframe e o nome de um campo e remove os espaços iniciais e finais de todos os valores possíveis'''
    return df.apply(
        lambda x: str(x[campo]).strip(),
        axis=1,
    )

def _normaliza_class(df):
    '''Remove o ponto final do campo class do dataframe'''
    return df.apply(
        lambda x: str(x['class']).strip().replace('.',''),
        axis=1,
    )

def _limpeza(df):
    ''' Realiza a limpeza do Dataframe removendo espaços de campos, textos. Remove também os registros inválidos
        Remove valores nominais e continuos inválidos'''
    df['workclass'] = _remove_espacos(df,'workclass')
    df['education'] = _remove_espacos(df,'education')
    df['marital_status'] = _remove_espacos(df,'marital_status')
    df['native_country'] = _remove_espacos(df,'native_country')
    df['relationship'] = _remove_espacos(df,'relationship')
    df['hours_per_week'] = _remove_espacos(df,'hours_per_week')
    df['occupation'] = _remove_espacos(df,'occupation')
    
    df['sex'] = _remove_espacos(df,'sex')
    df['race'] = _remove_espacos(df,'race')
    df['class'] = _normaliza_class(df)
    
    
    df = df.loc[
             df['workclass'].isin(LIST_WORKCLASS) & 
             df['education'].isin(LIST_EDUCATION)  & 
             df['marital_status'].isin(LIST_MARITAL_STATUS) &
             df['relationship'].isin(LIST_RELATIONSHIP) &
             df['race'].isin(LIST_RACE) &
             df['sex'].isin(LIST_SEX) &
             df['native_country'].isin(LIST_NATIVE_COUNTRIES) & 
             df['class'].isin(LIST_CLASS) 
           ] 
    df = df[pd.to_numeric(df['age'], errors='coerce').notnull()]
    df = df[pd.to_numeric(df['fnlwgt'], errors='coerce').notnull()]
    df = df[pd.to_numeric(df['education_num'], errors='coerce').notnull()]
    df = df[pd.to_numeric(df['capital_gain'], errors='coerce').notnull()]
    df = df[pd.to_numeric(df['capital_loss'], errors='coerce').notnull()]
    df = df[pd.to_numeric(df['hours_per_week'], errors='coerce').notnull()]
    df["age"] = pd.to_numeric(df["age"])
    df["fnlwgt"] = pd.to_numeric(df["fnlwgt"])
    df["hours_per_week"] = pd.to_numeric(df["hours_per_week"])
    return df

def ler_arquivo(diretorio, **kwargs):
    ''' Realiza a busca dos arquivos de um diretório e grava um arquivo temporário adult.csv'''
    df , arquivos= _busca_arquivos(diretorio)
    kwargs['ti'].xcom_push(key='arquivos',value=arquivos)
    
    filename = '/opt/airflow/files/adult.csv' 
    df.to_csv(filename, sep=',', index=False, header=False, quotechar='"',quoting=csv.QUOTE_NONNUMERIC)
    kwargs['ti'].xcom_push(key='arquivo_csv',value=filename)

def gravar_dados(**kwargs):
    ''' Realiza a leitura do arquivo temporário e grava no Postgres, tabela public.dados'''
    registros = 0
    idx = 0
    postgres_hook = PostgresHook(postgres_conn_id='conn_postgres')
    sql = """COPY public.dados FROM STDIN DELIMITER ',' CSV QUOTE '"' """
    postgres_hook.copy_expert(sql, kwargs['ti'].xcom_pull(key='arquivo_csv'))
        
        
def remover_arquivo(**kwargs):
    ''' Remove o arquivo temporário e renomeia os arquivos de entrada adicionando um prefixo .prc'''
    arquivos = kwargs['ti'].xcom_pull(key='arquivos')
    for arquivo in arquivos:
        os.rename(arquivo, f"{arquivo}.prc")
    os.remove(kwargs['ti'].xcom_pull(key='arquivo_csv'))