Documentação Arquitetura de dados 

Conforme a imagem anexada da arquitetura de dados mostra os dados vêm do arquivo Adult.data e salvo no SQLite através de um script em python orquestrado pelo crontab do Linux com o seguinte comando que roda de 10s em 10s 

* * * * * /home/eloahnoguchi/clicksign/python-dev-test/adult/main.py
* * * * * sleep 10  && /home/eloahnoguchi/clicksign/python-dev-test/adult/main.py
* * * * * sleep 20 && /home/eloahnoguchi/clicksign/python-dev-test/adult/main.py
* * * * * sleep 30 && /home/eloahnoguchi/clicksign/python-dev-test/adult/main.py
* * * * * sleep 40 && /home/eloahnoguchi/clicksign/python-dev-test/adult/main.py
* * * * * sleep 50 && /home/eloahnoguchi/clicksign/python-dev-test/adult/main.py


O script tem como função transformar o dado para que seja possível a manipulação do mesmo no banco de dados, as transformações realizadas foram:
Adição dos nomes das colunas;
Remoção de linha duplicada

Obs: Há alguns campos que estão com o tipo do campo como string, o que possibilita a inserção de qualquer informação, identifiquei alguns dados diferentes em campos tipo string, como por exemplo o campo “capital-gain” que possui um “ A” onde deveria ter informações de ganho de capital individual.
