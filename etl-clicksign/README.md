## Estrutura do projeto

#### Gerenciamento de dependências: *Poetry*

### Artefatos

1. Banco de dados (adult)
    - Model Class com SQLite e Peewe ORM (etl_clicksign/models.py)
2. DAG Airflow que carrega os dados no banco
    - Todo processo ETL 3 tasks(etl_clicksign/dag.py)
        * **create_table_sqlite_task** que inicializa criando a tabela caso ela nao exista
        * **extract** responsavel por extrair os dados dentro do split pedido 1.630 linhas por execução
        * **transform_load** responsáveil por carregar e inserir os dados no banco (utilizando o ORM) 
3. Testes unitários
    - Model Tests usando mocks, assertando o objeto no banco, limpando o banco e manipulando os dados do Adult.data com pandas(tests/test_models.py)
    - Um arquivo para testar durante o desenvolvimento algumas funcionalidades mais triviais do programa
4. Exploração de dados 
    - Exploração simples de dados usando jupyter (adult_data.ipynb)