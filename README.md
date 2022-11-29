# ETL_clicksign

## 💻 Projeto

Esse projeto roda uma pipeline ETL de um conjunto de dados da US Census Bureau, seguindo as especificações: https://github.com/clicksign/python-dev-test/blob/master/README.md


## Considerações sobre o projeto

1. Esse projeto executa um script que roda a cada 10 segundos e faz o ETL dos dados Adult.data e Adult.test, que estão dentro da pasta /data
2. A cada iteração ele processa 1630 registros do conjunto de dados.
3. Enquanto os dados não são carregados por completo no banco, um arquivo temporário é gerado no mesmo diretório, sendo utilizado como um buffer
4. Optei por utilizar o bilioteca schedule para agendamento da execução a cada 10 segundos pela simplicidade
5. Também utilizao o SQLAlchemy pela facilidade em inserir dados no banco. (Obs: Utilizo o metoho bulk_insert_mappings() da session do SQLAlchemy pois durante os testes ele apresentar um performance bem superior ao método add())
6. o arquivo ETL.ipynb contém a análise exploratório dos dados. A partir dele que foi desenvolvido toda a lógica do ETL.
7. Utilizo variável de ambiente para definir o caminho absoluto dos arquivos e os dados do banco. No arquivo .env-example você pode encontrar como criar um arquivo .env para rodar no seu local (obs: Sem em arquivo .env não vai funcionar)


## Os arquivos estão organizados da seguinte maneira:

- ETL.ipynb -> Análise exploratória dos dados
- etl
  - contants.py -> As colunas da tabela como Enum
  - service.py -> principal arquivo, onde é executado toda a lógica do ETL
- infractructure
  - core
    - models.py -> Representação do modelo de dados
    - repository.py -> Insersão dos dados no banco
    - settings.py -> Contém algumas variáveis de configurações
  - database
    - base.py
    - init_db.py -> Inicializa a tabele `adult` no banco
    - session.py -> Engine e session do SQLAlchemy
- script
  - run.py -> Script que roda o ETL a cada 10 segundos


## 🚀 Technologies

Este projeto foi desenvolvido com as seguintes tecnologias e ferramentas:
- Python 3.10.8
- Pandas 1.5.2
- Schedule 1.1.0
- SQLAlchemy 1.4.41
- Makefile

O banco de dados utilizado para carregar os dados foi o `postgreSQL`.


## Observações

Nesse projeto optei por utilizar um ambiente virtual e não um conteiner Docker pela facilidade.
E como ele carregada os dados em um banco postgres local, tenha em mente que você precisa ter um banco postgres rodando localmente.


## ℹ️ Como rodar o projeto

1. Preparando o ambiente:

Você pode usar um [virtualenv](https://virtualenv.pypa.io/en/latest/) para executar o aplicativo localmente.

Virtualenv já está incluído na biblioteca padrão do Python3. Você pode criar um ambiente virtual usando o comando abaixo:

```
python3 -m venv venv
```

Ative seu ambiente virtual
```
(Unix ou MacOS) $ source venv/bin/activate
(Windows) ...\> env\Scripts\activate.bat
```

Com o ambiente virtual ativado, instale as dependências, utilizando o comando abaixo:
```
make install-dependencies
```

Não esqueça de criar uma arquivo `.env`. Você pode renomear o arquivo `.env.example` para `.env` e mudar o valor das variáveis.
O valor da variável BASE_PATH deve ser o caminho da pasta do projeto (no meu caso o meu projeto está dentro de /clicksign)


Rode o script
```
make run-etl
```

Agora você pode acompanhar pelo terminal a execução do script


## 📎 Versioning

1.0.0

## 🧔 Authors

* **Marco Capozzoli**: e-mail: marcocapozzoli90@gmail.com