# Estratégia
 Optei por usar multiprocessamento assíncrono para acelerar os processos de ETL.
 Tentei ao máximo ulitilzar apena libs nativas do python.
 Escolhi por usar as Lib's externas aiofiles e aiosqlite para lidar com a leitura/escrita assíncrona dos arquivos/base
 e a lib Rocketry para o agendamento das execuções

# Como executar

1. Instale as dependências do projeto com ````pip install -r requirements.txt````
2. Rode o comando: ````python main.py '.\path\to\file' numero_de_linhas_para_processar```` 
