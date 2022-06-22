# Estudo de caso

Foi buscado neste projeto ser efetivo, focando na funcionalidade, tempo e evitar problemas de compatibilidade ou dificuldade de execução.

Dado a simplicidade do desafio, o código foi realizado integralmente em python, sem utilização de frameworks como no caso do Apache AirFlow visto que para o caso em questão não haveria necessidade e aumentaria possível problema de compatibilidade.

# Pacotes necessários

pip install python-time
pip install SQLAlchemy
pip install pandas

# Processo ETL

Priorizei assim a efetividade e tempo de execução, onde em todo o processo de ETL não foi utilizado nenhum looping for, sendo todo o processo executado em aproximadamente 2 segundos.

Os tabelas Adult.data e Adult.txt unidas em um único dataframe, sendo este dataframe a fonte de nossos dados brutos(source). Como também foi criado o datafrane starea (staging) a qual foram alterados sem prejudicar os dados fontes, conforme modelagem típica para implantação de dataware house.

Utilizei o próprio python para fazer análise exploratória de dados, verificando valores discrepantes nos valores em cada coluna.

Verifiquei assim que em geral havia espaços em branco nos valores e alguns dados errôneos. Sendo assim utilizei funções próprias para tratamento de string e padronização dos dados errôneos.

Exemplo: 

df_starea['workclass'] = df_starea['workclass'].apply(lambda x:  "<error>" if ('?' in x) else x.strip() ) 

Toda as linhas do arquivo estão comentadas explicitando devidadamente o procedimento em cada linha de código.

Após o processo de ETL ter sido feito, foi realizado a conexão com o banco de dados postgre, utilizando a biblioteca sqlalchemy.

#Integração com o banco

Para conectar com outro banco postgree que esteja on premisses, basta alterar as linhas 145,146,147 e 148, especificando o usuário, senha, porta e nome da dataabase.

port='5433' #porta do banco
user = 'postgres' #user do banco
password= '123' #senha do banco
bd_name ='test' #nome da database

Foi criado assim os comandos SQL para serem executados dentro do python, tanto para definição das chaves primárias e para criação das views sem os dados errôneos.

Exemplos:

#Cria a chave primária
sqlcomand="""ALTER TABLE public."Adult_data"
  ADD CONSTRAINT id_data 
    PRIMARY KEY (index);"""

#Cria a view apenas com os válores válidos
sqlcomand3= """CREATE VIEW data_only_valid AS
SELECT * FROM public."Adult_data"
    WHERE "native-country" <> '<error>' and age > 0 and workclass <> '<error>' and occupation <> '<error>' and "capital-gain" <> -99 and "hours-per-week" <> -99;"""

Para o tratamento dos dados em lote e inserção no banco de dados, foi utilizado a função do pandas to_sql(), onde ela pega a conexão já feita com o sqlalchemy e insere os dados com velocidade os dados no banco.

Foi criado assim um controle de lote, para inserção de apenas 1630 registros (linhas) por vez e aguarda 10 segundos para a próxima inserção.

Exemplo:

for i in range(0,len(df_data_clean),1630):

	#por questões de velocidade e praticidade para inserção, foi utilizado o método pandas.to_sql()

	df_data_clean[i:(i+1630)].to_sql(name='Adult_data', con=engine, if_exists='append', index=True , dtype={'index': sqlalchemy.types.INTEGER(), 'age': sqlalchemy.types.INTEGER(),'workclass': sqlalchemy.types.VARCHAR(length=50),
	'fnlwgt': sqlalchemy.types.INTEGER(),
	'education': sqlalchemy.types.VARCHAR(length=50),
	'education-num':  sqlalchemy.types.INTEGER(),
	'marital-status' :sqlalchemy.types.VARCHAR(length=50),
    'occupation': sqlalchemy.types.VARCHAR(length=50),
    'relationship': sqlalchemy.types.VARCHAR(length=50),
    'status': sqlalchemy.types.VARCHAR(length=50),
     race':  sqlalchemy.types.VARCHAR(length=50),
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


Nome das views: **data_only_valid** e **test_only_valid**.

Creio assim que todas as tarefas demandadas foram efetivadas, podendo ainda serem executadas de diversas formas, como realizar o processo de ETL dentro do próprio banco, utilizar o airflow para orquestração ou com alguma ferramenta de integração de dados como o Pentaho, ODI entre outras.


Caso seja desejado que se faça o projeto de forma mais robusta, será um prazer, porém, dado minha experiência considerei realizar o mais efetivo e menos propensos a erro, para o caso em questão.











