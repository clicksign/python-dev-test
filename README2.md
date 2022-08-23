# Teste Python-Dev

Para esse projeto de ETL, foi pedido que fosse feita a extração de dados de um arquivo .data e que estes dados fosse enviados a uma instância de um bando de dados Postgres ou SQlite.
Decidi por utilizar o **Postgres em um ambiente dockerizado**.

As configurações DB:
1. DATABASE_URL=psql://postgres:root@db-dev:5432/python-dev
2. POSTGRES_USER: "postgres"
3. POSTGRES_PASSWORD: "root"
4. POSTGRES_DB: "python-dev"

E para interagir com com esse banco de dados, criei uma estrutura no Django Rest Framework (DRF), onde são disponibilizadas algumas rotas para realizar a criações dos registros no BD.


Inicialmente analisei como os dados estavam vindo no Jupyter Notebook e após uma primeira ideia, tracei quais planos utilizar.
[imagem df]

A partir do arquivo de descrição, identifiquei qual o tipo de dado era esperado por cada campo da fonte de dados (Adult.data) e segui com tratamos de verificação se, por exemplo, no campo idade apenas constavam valores numerais, caso contrário iria criar alguma estratégia com aquele registro.
[imagem regex]

E assim por diante para todos os demais campos.

Como a carga pedida era de lotes de 1630 registros a cada 10s, tive que pensar em uma estratégia para garantir o estado (ponto de onde parou). 
[imagem get_data]

E para solucionar isso, além da model que iria receber os dados enviados da fonte de dados (CensusEtl), para o DRF, criei uma outra model para armazenar um contador (Counter) e poder identificar o ponto de partida para a nova carga, acessar os endpoints e enviar um post ao banco.
[imagem payload]

Para rodar a rotina de ingestão dos dados no banco, criei um script que é um comando dentro da estrutura do Django que realiza a ingestão de acordo com o tamanho do lote e com o tempo desejado, semelhante a uma crontab.

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
