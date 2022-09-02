## Estrutura do projeto

#### Gerenciamento de dependências: *Poetry*

### Artefatos

1. Banco de dados (adult)
    - Model Class com SQLite e Peewe ORM (etl_clicksign/models.py)
2. Cronjob que carrega os dados no banco
    - ETL (etl_clicksign/etl.py)
3. Testes unitários
    - Model Tests usando mocks, assertando o objeto no banco e limpando o banco (tests/test_models.py)
4. Exploração de dados 
    - Exploração simples de dados usando jupyter (adult_data.ipynb)