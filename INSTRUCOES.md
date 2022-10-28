# ClickSign ETL

versao python 3.9.0

## Pré-requisitos

### 1.Iniciando Docker

Na raiz do projeto existe um arquivo `docker-compose.yml` para rodar, precisamos ter instalado o docker e docker-compose. Para rodar o docker-compose:


    sudo docker-compose up # existe a opcao -d para deixar o docker rodando em segundo plano
    
Estamos subindo 2 containers, o primeiro com o **postgresql** e um outro com o **pgadmin4** (o link para acesso ficou **localhost:16543** com email de **admin@admin.com** e senha de **root**).

Depois de logar no pgadmin, precisamos adicionar a conecao com o postgresql, o **host -> postgres-compose**, **user -> postgres**, **password -> root** e **port -> 5432**.

Apos a inicializacao dos containers, apenas crie o banco de dados com nome de *adult*, o resto sera feito pelas migrations.

### 2.Virtual Enviroment

Primeiro preencha as váriaveis no `.env.example` e retire o **.example** do arquivo para criar um `.env`. Depois:

    #Crie e ative o virtual env
    python -m venv venv
    source venv/bin/activate
    
    #Instalando dependencias
    pip install -r requirements.txt

### 3.Executando as Migrations

Agora precisamos executar um comando alembic para rodar as migrations:


    alembic upgrade head

### 4.Crontab e Execucao Manual

Para rodar o crontab, apenas **mude o caminho do projeto** no arquivo `crontab` na raiz do projeto. Para rodar manualmente:

    python application.py process main