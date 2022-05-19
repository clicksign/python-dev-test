## Crie e ative um ambiente virtual para isolar as dependências:

Obs: No desenvolvmento foi utilizado a versão Python 3.10.4

```
virtualenv .venv -p python3

source .venv/bin/activate
```

## Instale as dependências:

Pandas e Jupyter lab para executar e visualar o estudo dos dados com a explicação da origem de todas as decisões de processamentos realizados nas DAGs, além dos próprios processamentos nas DAGs

```
pip install jupyterlab pandas
```

Para visualizar o notebook digite o comando a seguir e acesse a url gerada:

```
jupyter lab
```

Airflow para execução das DAGs e realização do processo de ETL:

Obs: Muito importante executar os comandos a seguir na raiz desse projeto, caso contrário não irá executar!

```
export AIRFLOW_HOME=$PWD/airflow

AIRFLOW_VERSION=2.3.0

PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Rode o comando a seguir para que o airflow realize uma configuração básica, mas suficiente para executar nosso código:

```
airflow standalone
```

O próprio airflow irá gerar um username que geralmente é "admin" e uma senha aleatória. Acesse o link [](http://localhost:8080) e utilize as credenciais geradas pelo airflow para fazer login.

Após realizar o login ative a nossa DAG clicando no botão à esquerda do nome da mesma, que é "etl_census_bureau".

A partir desse momento o airflow irá disparar a DAG responsável pelo processamento dos dados.