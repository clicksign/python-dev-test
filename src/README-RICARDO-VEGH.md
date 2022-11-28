# Acessando a database
    Basta realizar o download do sqlitebrowser, realizar a instalação, e após aberto, adicionar a database adult na pasta src através da opção
    ''OPEN DATABASE'' no menu superior e visualizar a tabela através de SQL convencional.

#Ordem de excecução:
    Para melhor visualização do processo, pode-se excluir o arquivo adult.db e realizar a seguinte sequência:
        database.py -- recria a database e a tabela adult do zero.
        insert_data.py -- insere 1630 registros e executa a cada 10 segundos(Como no passo a passo abaixo, pode-se configurar um cronjob pelo crontab para simular a execução. Por padrão, o script está rodando todo dia as 10h05).

# Como configurar um cronjob via crontab no linux:
    Abra o terminal dentro do repositório e digite a seguinte sequência de comandos:
    sudo chmod +x ./path_do_arquivo > crontab -e, 5 10 * * * ./path_do_arquivo > ctrl x > enter > crontab -l(mostra os crons configurados)

# Como instalar o sqlitebrowser para visualização do banco de dados, tabela, e dados inseridos:
    https://sqlitebrowser.org/dl/