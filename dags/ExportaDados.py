def exporta_dados():

    import psycopg2

    host = '172.23.0.3'
    port = 5432
    db_name = 'postgres'
    username = 'edson'
    password = 'clicksign'

    # Criando conexão
    conn = psycopg2.connect(host = host, port = port, dbname = db_name, user = username, password = password)

    # Criando cursor
    cur = conn.cursor()

    # executar query (criando tabela)

    cur.execute("""DROP TABLE IF EXISTS clicksign;
        CREATE TABLE clicksign(
        age                   INTEGER,
        workclass             VARCHAR(20),
        fnlwgt                DECIMAL,
        education             VARCHAR(20),
        education_num         INTEGER,
        marital_status        VARCHAR(30),
        occupation            VARCHAR(20),
        relationship          VARCHAR(30),
        race                  VARCHAR(30),
        sex                   VARCHAR(10),
        capital_gain          DECIMAL,
        capital_loss          DECIMAL,
        hours_per_week        INTEGER,
        native_country        VARCHAR(30),
        class                 VARCHAR(10),
        id                    INTEGER NOT NULL PRIMARY KEY)""")

        # Em caso de erro na célula anterior	
        #cur.execute('rollback;')

    def send_csv_to_psql(connection,csv,table_):
        sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
        file = open(csv, "r")
        table = table_
        with connection.cursor() as cur:
            cur.execute("truncate " + table + ";") 
            cur.copy_expert(sql=sql % table, file=file)
            connection.commit()
        return connection.commit()

    send_csv_to_psql(conn,'/usr/local/airflow/dags/clicksign_df.csv','clicksign')
    conn.commit()

    # Fechando cursor

    cur.close()

    # Fechar conexão

    conn.close()