# Teste Python-Dev

Para esse projeto de ETL, foi pedido que fosse feita a extração de dados de um arquivo .data e que estes dados fosse enviados a uma instância de um banco de dados Postgres ou SQlite.
Decidi utilizar o **Postgres em um ambiente dockerizado**.

As configurações DB:
1. DATABASE_URL=psql://postgres:root@db-dev:5432/python-dev
2. POSTGRES_USER: "postgres"
3. POSTGRES_PASSWORD: "root"
4. POSTGRES_DB: "python-dev"

E para interagir com com esse banco de dados, criei uma estrutura no Django Rest Framework (DRF), onde são disponibilizadas algumas rotas para realizar as criações dos registros no BD.


Inicialmente analisei como os dados estavam vindo no Jupyter Notebook e após uma primeira ideia, tracei quais planos utilizar.
![alt text](https://github.com/Bereoff/python-dev-test/blob/bruno_bereoff/images/df_jupyter.png "análise prévia dos dados")

A partir do arquivo de descrição, identifiquei qual o tipo de dado era esperado por cada campo da fonte de dados (Adult.data) e segui com tratamentos de verificação. Por exemplo, se no campo "age" apenas constavam valores numéricos, caso contrário iria criar alguma estratégia com aquele registro.
![alt text](https://github.com/Bereoff/python-dev-test/blob/bruno_bereoff/images/df_regex_jupyter.png "verificação coerência dados de acordo com o campo")

E assim por diante para todos os demais campos.

Como a carga pedida era de lotes de 1630 registros a cada 10s, tive que pensar em uma estratégia para garantir o estado (ponto de onde parou). 
![alt text](https://github.com/Bereoff/python-dev-test/blob/bruno_bereoff/images/desev_jupyter.png "função para batch de dados")
E para solucionar isso, além da model que iria receber os dados enviados da fonte de dados (CensusEtl), para o DRF, criei uma outra model para armazenar um contador (Counter) e poder identificar o ponto de partida para a nova carga, acessar os endpoints e enviar um post ao banco.
![alt text](https://github.com/Bereoff/python-dev-test/blob/bruno_bereoff/images/testes_jupyter.png "payload enviado no post dos dados no banco")

Para rodar a rotina de ingestão dos dados no banco, criei um script que é um comando dentro da estrutura do Django que realiza a ingestão de acordo com o tamanho do lote e com o tempo desejado e que necessita ser acionada manualmente.

Sobre os passos de execução para rodar o projeto:

* clonar o repositório

*  python -m venv venv

*  pip install -r requirements

* docker-compose up -d

* python manage.py migrate

E o projeto estará pronto para utilizar.

O após estas etapas é necessário iniciar o servidor com:
* python manage.py runserver 8001

E para realizar a ingestão do dados no banco:
* python manage.py ETL_Django

E para uma deleção rápida nas duas models (CensusEtl e Counter)
* python manage.py delete_models

Os endpoints criados são:
* http://localhost:8001/admin/ (Admin DRF)
* http://localhost:8001/api/v1/census-etl/ (CensusEtl)
* http://localhost:8001/api/v1/census-etl/counter  (Counter)

Gostaria de agradecer pela oportunidade de participar deste processo! 🙌 😄
