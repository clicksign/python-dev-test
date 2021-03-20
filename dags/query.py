def query():

    import psycopg2
    import math
    import pandas as pd
    import time

    host = '172.23.0.3'
    port = 5432
    db_name = 'postgres'
    username = 'edson'
    password = 'clicksign'

    # Criando conexão
    conn = psycopg2.connect(host = host, port = port, dbname = db_name, user = username, password = password)

    # Criando cursor
    cur = conn.cursor()

    # Executar query (criando tabela)

    df = pd.read_csv('/usr/local/airflow/dags/clicksign_df.csv')
    linhas = df.shape[0]
    limite = 1630
    repeticoes = math.ceil(linhas/limite)
    

    def query_limite(limite_inferior, limite_superior):


        cur.execute("""SELECT * FROM clicksign WHERE id>{} AND id<={};""".format(limite_inferior, limite_superior))

                
        cols = [ desc[0] for desc in cur.description]

        tables_name = []
        for i in cur:
            tables_name.append(i)

        tables_name = pd.DataFrame(tables_name, columns = cols)

        return tables_name


    limite_inferior = 0
    limite_superior = 1630

    for i in range(repeticoes):
        table = query_limite(limite_inferior, limite_superior)
        limite_inferior = limite_inferior + 1630
        limite_superior = limite_superior + 1630
        table.to_csv('~/dags/Table_{}.csv'.format(i), index=False)
        time.sleep(10)

    

    # Fechando cursor

    cur.close()

    # Fechar conexão

    conn.close()