import sqlite3

def return_data(table_name):
    """
    Essa função conecta com a tabela criada e executa uma consulta na tabela, retornando os dados inseridos
    """
    conn = sqlite3.connect(f'{table_name}.db')
    cursor = conn.cursor()
    data=cursor.execute(f'''SELECT * FROM {table_name}''')
    return data