requisitos

python 3.8.10
airflow 2.1.2
jupyter notebook

instalação via Linux ou Mac:
python3 -m venv .env
source .env/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
pip install apache-airflow==2.1.2

airflow db init

Passo a passo

criar uma pasta com o nome dags dentro da pasta airflow
direcionar o arquivo 

Após isso, executar o jupyter notebook através do arquivo ETL.ipynb
source .env/bin/activate
jupyter notebook --port 8895

iniciar airflow webserver

source .env/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080

iniciar o airflow scheduler

source .env/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler

No arquivo carrega_dados.py está a parte final do ETL,na qual seria a transformação de dados, em paralelo ao arquivo referente à DAG do apache airflow 
