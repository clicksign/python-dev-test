import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sqlalchemy as sqlalchemy
import time as time
#pip install python-time
#pip install SQLAlchemy


#setando as colunas
tempo_ini= time.time()
columns = ['age', 'workclass', 'fnlwgt','education','education-num','marital-status','occupation','relationship','race','sex','capital-gain','capital-loss','hours-per-week','native-country','class']

#Upando os arquivos para fazer análise exploratória e processo de ETL
df_data_source = pd.read_csv('./data/Adult.data',header=None, names=columns)
df_test_source = pd.read_csv('./data/Adult.test',header=None, names=columns,skiprows=1)

#df_data_source tem indice de 0 até 32560
#df_test_source tem indice de 32561 até ...

#juntando ambas as tabelas para processo único de etl
df_starea = df_data_source.append(df_test_source)
df_starea= df_starea.reset_index()

#Análise de dados para processo de ETL

#é verificado inconsistências com os dados e tipagem diferentes

df_starea['age'].unique()

#Como temos valores com erro e formatos diferentes, converteremos tudo em string 
df_starea['age'] = df_starea['age'].apply( lambda x: str(x) )

#Agora que temos tudo em string, veremos se o conteúdo da mesma é do tipo númerico, caso contrário, saberemos que há um dado errôneo a qual renomearemos por -99

df_starea['age'] = df_starea['age'].apply(lambda x:  int(x) if (x.isnumeric() == True) else -99 ) 

#Coluna workclass possui dados com espaços em branco e válores faltantes

df_starea['workclass'].unique()

df_starea['workclass'] = df_starea['workclass'].apply(lambda x:  "<error>" if ('?' in x) else x.strip() ) 

#Coluna fnlwgt é verificado espaços em brancos e valores missing

#Converte todos os objetos em tipo string
df_starea['fnlwgt'] = df_starea['fnlwgt'].apply( lambda x: str(x) )

#Verifica se existe espaço em branco e valores missing em valor -99
df_starea['fnlwgt'] = df_starea['fnlwgt'].apply(lambda x:  int(x) if (x.strip().isnumeric() == True) else -99 ) 

df_starea['fnlwgt'].hist()

#Coluna education possui espaços em branco

df_starea['education'].unique()

#Retira os espaços em branco
df_starea['education'] = df_starea['education'].apply( lambda x: x.strip())

#Coluna education-num não possui erros
df_starea['education-num'].unique()

#Coluna marital-status com espaços em branco
df_starea['marital-status'].unique()

#Retira os espaços em brancos
df_starea['marital-status']= df_starea['marital-status'].apply(lambda x: x.strip())

#Coluna occupation com valores faltando e espaços em branco
df_starea['occupation'].unique()

#Retira os valores missing convertendo para <error> e retira os espaços em branco
df_starea['occupation'] = df_starea['occupation'].apply(lambda x:  "<error>" if ('?' in x) else x.strip() ) 

#Coluna relationship com valores em branco

df_starea['relationship'].unique()

#Retira os espaços em branco
df_starea['relationship'] = df_starea['relationship'].apply(lambda x : x.strip())

#Coluna race com espaços em branco
df_starea['race'].unique()

#Retira os espaços em branco
df_starea['race'] = df_starea['race'].apply(lambda x : x.strip())

#Coluna sex com espaços em branco
df_starea['sex'].unique()

df_starea['sex'] = df_starea['sex'].apply(lambda x : x.strip())

#Coluna capital-gain com valores em branco e valores errados

df_starea['capital-gain'].unique()

#Converte tudo em String
df_starea['capital-gain']= df_starea['capital-gain'].apply(lambda x : str(x).strip())

#Verifica se existe espaço em branco e valores missing em valor -99
df_starea['capital-gain']= df_starea['capital-gain'].apply(lambda x:  int(x) if (x.strip().isnumeric() == True) else -99 ) 

#Coluna capital loss não foi verificado erro

df_starea['capital-loss'].unique()

df_starea['capital-loss'].hist()

#Coluna hours-per-week com espaços em branco, valores errados com tipos diferentes

df_starea['hours-per-week'].unique()

#Converte tudo em String
df_starea['hours-per-week']= df_starea['hours-per-week'].apply(lambda x : str(x).strip())

#Verifica se existe espaço em branco e valores missing em valor -99
df_starea['hours-per-week']= df_starea['hours-per-week'].apply(lambda x:  int(x) if (x.strip().isnumeric() == True) else -99 ) 

#Coluna native-country com espaços em branco e valores missing

df_starea['native-country'].unique()

#Retira os valores missing convertendo para <error> e retira os espaços em branco
df_starea['native-country']= df_starea['native-country'].apply(lambda x:  "<error>" if ('?' in x) else x.strip() ) 

#Coluna Class, valores em formatação errada e espaços em branco

df_starea['class'].unique()

df_starea['class']= df_starea['class'].apply(lambda x:   x.strip().replace('.',''))

#Processo de ETL terminado, fazendo o split da tabela para subir no banco

#voltando para as tabelas padrão
df_data_clean = df_starea[0:32561].iloc[:,1:]
df_test_clean = df_starea[32562:].iloc[:,1:]

df_test_clean = df_test_clean.reset_index().iloc[:,1:]

print('Processo de ETL terminado em', round(time.time()-tempo_ini,2), 'segundos.' )
print('Começando a conexão com o banco e ingestão de dados')
#Utiliza o pacote sqlalchemy para integração com o banco postgress

port='5433' #porta do banco
user = 'postgres' #user do banco
password= '123' #senha do banco
bd_name ='test' #nome da database

#faz a conexão com o banco
engine = create_engine( 'postgresql://{}:{}@localhost:{}/{}'.format(user,password,port,bd_name) )

#Cria a chave primária
sqlcomand="""ALTER TABLE public."Adult_data"
  ADD CONSTRAINT id_data 
    PRIMARY KEY (index);"""

#Cria a chave primária
sqlcomand2="""ALTER TABLE public."Adult_test"
  ADD CONSTRAINT id_test 
    PRIMARY KEY (index);"""

#Loop para subir os dados para o banco e definir tipo de dados em cada coluna

for i in range(0,len(df_data_clean),1630):

	#por questões de velocidade para inserção, foi utilizado o método pandas.to_sql()

	df_data_clean[i:(i+1630)].to_sql(name='Adult_data', con=engine, if_exists='append', index=True , dtype={'index': sqlalchemy.types.INTEGER(), 'age': sqlalchemy.types.INTEGER(),'workclass': sqlalchemy.types.VARCHAR(length=50),
	'fnlwgt': sqlalchemy.types.INTEGER(),
	'education': sqlalchemy.types.VARCHAR(length=50),
	'education-num':  sqlalchemy.types.INTEGER(),
	'marital-status' :sqlalchemy.types.VARCHAR(length=50),
  'occupation': sqlalchemy.types.VARCHAR(length=50),
  'relationship': sqlalchemy.types.VARCHAR(length=50),
  'status': sqlalchemy.types.VARCHAR(length=50),
  'race':  sqlalchemy.types.VARCHAR(length=50),
  'sex': sqlalchemy.types.VARCHAR(length=50),
  'capital-gain': sqlalchemy.types.Float(precision=3, asdecimal=True),
  'capital-loss': sqlalchemy.types.Float(precision=3, asdecimal=True),
  'hours-per-week': sqlalchemy.types.INTEGER(),
  'native-country':sqlalchemy.types.VARCHAR(length=50),
  'class' :sqlalchemy.types.VARCHAR(length=50)
	 	
	} )
	#aguarde 10 segundos, para fazer a nova inserção
	time.sleep(10)
	print('1630 dados inseridos, aguarde 10 segundos')




#verica o quanto faltou para inserção dos 1630 dados e insere já os dados da tabela teste
rest= (i+1630)- len(df_data_clean)

df_test_clean[:(rest)].to_sql(name='Adult_test', con=engine, if_exists='append', index=True, dtype={'index': sqlalchemy.types.INTEGER(), 'age': sqlalchemy.types.INTEGER(),'workclass': sqlalchemy.types.VARCHAR(length=50),
	'fnlwgt': sqlalchemy.types.INTEGER(),
	'education': sqlalchemy.types.VARCHAR(length=50),
	'education-num':  sqlalchemy.types.INTEGER(),
	'marital-status' :sqlalchemy.types.VARCHAR(length=50),
  'occupation': sqlalchemy.types.VARCHAR(length=50),
  'relationship': sqlalchemy.types.VARCHAR(length=50),
  'status': sqlalchemy.types.VARCHAR(length=50),
  'race':  sqlalchemy.types.VARCHAR(length=50),
  'sex': sqlalchemy.types.VARCHAR(length=50),
  'capital-gain': sqlalchemy.types.Float(precision=3, asdecimal=True),
  'capital-loss': sqlalchemy.types.Float(precision=3, asdecimal=True),
  'hours-per-week': sqlalchemy.types.INTEGER(),
  'native-country':sqlalchemy.types.VARCHAR(length=50),
  'class' :sqlalchemy.types.VARCHAR(length=50)
	 	
	} ) 
print('1630 dados inseridos, aguarde 10 segundos')
time.sleep(10)

#insere os dados de treino em lotes de 1630 aguardando 10 segundos.
for i in range((rest), len(df_test_clean), 1630):
	df_test_clean[i:(i+1630)].to_sql(name='Adult_test', con=engine, if_exists='append', index=True )
	time.sleep(10)
	print('1630 dados inseridos, aguarde 10 segundos')


#executa o comando sql para criar as chaves primárias
engine.execute(sqlcomand)
#executa 0 comando sql para criar as chaves primárias
engine.execute(sqlcomand2)

#Cria a view apenas com os válores válidos
sqlcomand3= """CREATE VIEW data_only_valid AS
SELECT * FROM public."Adult_data"
    WHERE "native-country" <> '<error>' and age > 0 and workclass <> '<error>' and occupation <> '<error>' and "capital-gain" <> -99 and "hours-per-week" <> -99;"""

#Cria a view apenas com os válores válidos
sqlcomand4= """CREATE VIEW test_only_valid AS
SELECT * FROM public."Adult_test"
    WHERE "native-country" <> '<error>' and age > 0 and workclass <> '<error>' and occupation <> '<error>' and "capital-gain" <> -99 and "hours-per-week" <> -99;"""

engine.execute(sqlcomand3)
engine.execute(sqlcomand4)
engine.execute('commit;')


