# python-dev-test

Como solução proposta ao teste foi criada uma solução, utilizando o SO Windows 10, com as seguintes caracteristiscas:<br>
    &nbsp;\- Processo de ETL com o Airflow<br>
    &nbsp;\- SGBD: Postgres:13

---
## Características do Processo

O processo foi configurado para rodar a cada hora (cron: 0 */1 * * *), com um intervalo de 1630 linhas por insert e um intervalo de 10 segundos para conjuntos de dados inseridos.

<b>Dados do Banco de dados</b><br>
    &nbsp;HOST=host.docker.internal<br>
    &nbsp;DATABASE=adult<br>
    &nbsp;USER=root<br>
    &nbsp;PASSWORD=root

<b>Criação de um arflow com as seguintes pastas: </b><br>
 &nbsp;\- \dags\
 
 &nbsp;\- \docker-files\
 &nbsp;\- \docker-files\docker-airflow\
 &nbsp;\- \docker-files\docker-db-postgres\

 &nbsp;\- \inputs\
 &nbsp;\- \inputs\data
 &nbsp;\- \inputs\queries 

 &nbsp;\- \logs\
 &nbsp;\- \plugins\ 
 &nbsp;\- \plugins\dataprocessing\
 &nbsp;\- \plugins\operators\
 &nbsp;\- \plugins\utils\

<b> Para configurar o processo do airflow, tem-se os arquivos: </b><br>
 &nbsp;\- \docker-files\docker-airflow\docker-compose<br>
 &nbsp;\- \docker-files\docker-airflow\Dockerfile<br>
 &nbsp;\- \docker-files\docker-airflow\requeriments.txt<br>
 &nbsp;\- \docker-files\docker-airflow\.env

<b> Para configurar o processo do postgres, tem-se os arquivos: </b><br>
 &nbsp;\- \docker-files\docker-db-postgres\docker-compose<br>
 &nbsp;\- \docker-files\docker-db-postgres\.env<br>

<b> Para criação da tabela, foi criado o script adult.sql: </b><br>
 &nbsp;\- \inputs\queries\create\adult.aql
 Script:
 ```
    create table if not exists public.adult (
        id_adult            serial primary key,
        age                 bigint,
        workclass           varchar(50),
        fnlwgt              bigint,
        education           varchar(50),
        "education-num"     bigint,
        "marital-status"    varchar(50),
        occupation          varchar(100),
        relationship        varchar(50),
        race                varchar(50),
        sex                 varchar(6),
        "capital-gain"      real,
        "capital-loss"      real,
        "hours-per-week"    bigint,
        "native-country"    varchar(50),
        class               varchar(5),
        dat_import          timestamp default now()
    )
 ```


<b>Para processar, temos o seguinte arquivo .py:</b><br>
 &nbsp;\- \plugins\dataprocessing\main.pyt
 Nessa pasta tem uma função main com os tratamentos utilizados para processar o arquivo
 Tratamentos utilizados:<br>
   &nbsp;&nbsp;\- Remoção de duplicatas<br>
   &nbsp;&nbsp;\- Tratamento de registros inconstentes<br>
   &nbsp;&nbsp;\- Tratamento de números nulos

<b>Para auxiliar no processo de ingestão, foram criadas os seguintes objetos:</b><br>
   &nbsp;\- DataToPostgresOperator: responsável por configurar os métodos de ingestão e com os seguintes parâmentros:<br><br>
        &nbsp;&nbsp;&nbsp;&nbsp;task_id: nome da task 
        &nbsp;&nbsp;&nbsp;&nbsp;method: método a ser executado, entre eles:<br>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\- execute<br>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\- truncate<br>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\- insert<br>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\- insert_df_pandas <br>
        &nbsp;&nbsp;&nbsp;&nbsp;conn_id: id da conexão armazenada dentro das connections do airflow<br>
        &nbsp;&nbsp;&nbsp;&nbsp;path_file: caminho do arquivo a ser executado<br>
        &nbsp;&nbsp;&nbsp;&nbsp;cols_type: nome e tipo das colunas <br>
        &nbsp;&nbsp;&nbsp;&nbsp;table_name: nome da tabela <br>
        &nbsp;&nbsp;&nbsp;&nbsp;range_data: intervalo de linhas a serem inseridas<br>
        &nbsp;&nbsp;&nbsp;&nbsp;step_time: intervalo de tempo para cada ingestão<br>
        &nbsp;&nbsp;&nbsp;&nbsp;delimiter: delimitador, caso a ingestão seja por um arquivo csv<br>
        &nbsp;&nbsp;&nbsp;&nbsp;encoding: encoding do arquivo

<b>Para executar os parâmentros do processo:</b><br>
 &nbsp;\- Dag com o caminnho: \dags\dag_file_to_postgres.py
 ```
    # Declated DAG with parameters
    dag = DAG(
        dag_id="data_to_postgres",
        schedule_interval = "0 */1 * * *",
        dagrun_timeout=datetime.timedelta(minutes=60),
        start_date = days_ago(0)
    ) 

    # Creating task to create table
    task_create_table = DataToPostgresOperator(
                        task_id = "task_create_table",
                        conn_id = "adult_db_id",
                        path_file = path_query,
                        method = "execute",
                        execution_timeout=datetime.timedelta(hours=2),
                        dag = dag
                    )
    # Creating a task to truncate table
    task_truncate_table = DataToPostgresOperator(
                        task_id = "task_truncate_table",
                        conn_id = "adult_db_id",
                        method = "truncate",
                        table_name = "adult",
                        execution_timeout=datetime.timedelta(hours=2),
                        dag = dag
                    )
    # Create task to oinsert table
    task_insert_data = DataToPostgresOperator(
                    task_id = "task_insert_data",
                    method = "insert_df_pandas",
                    table_name = "adult",
                    path_file = path_file,
                    cols_type = cols_type,
                    range_data = 1630,
                    conn_id = conn_id,
                    execution_timeout=datetime.timedelta(hours=2),
                    step_time = 0,
                    dag = dag   
                    )

    task_create_table >> task_truncate_table >> task_insert_data
 ```