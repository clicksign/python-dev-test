# Desafio - Dev Python

Este repositório possui um teste que visa avaliar sua curiosidade, seus conhecimentos em Python, análise e limpeza de dados, Storytelling e conceitos relacionados a processos ETL/ELT. O teste possui seu próprio conjunto de arquivos, parâmetros, instruções e estratégias para ser resolvido. Portanto, estude cada detalhe com sabedoria.

# US Census Bureau - Criação de um processo ETL/ELT

Sua tarefa é criar um processo ETL/ELT com agendamento que transporte dados úteis, presentes nos datasets fornecidos, para um banco de dados relacional. Os critérios para a execução deste desafio são:

1. Suas **únicas e excluisivas** fonte de dados devem ser os datasets fornecidos neste repositório;
2. Você deve processar **todos** os arquivos de dados fornecidos;
3. Seu script deve ser agendado para rodar a cada **10 segundos** processando **1.630 registros**;
4. Aplique todas as transformações e limpeza de dados que julgar necessária (*Tenha em mente que precisamos acessar dados úteis que possibilitem a extração de insights!*);
5. Carregue os dados processados em um banco de dados **Postgres ou SQLite** e;
6. Ao criar sua tabela no banco de dados, respeite a **tipagem dos dados e o nome das colunas** fornecidas no arquivo de descrição.

# Dicas

(:gem:) Facilite sua vida! Use alguma tecnologia de agendamento como o Apache *Airflow* ou até mesmo o *Crontab* do Linux.

# Instruções

Por favor, desenvolva um script ou programa de computador utilizando a linguagem de programação **Python** para resolver o problema proposto. Estamos cientes da dificuldade associada a tarefa, mas toda criatividade, estratégia de raciocínio, detalhes na documentação do código, estrutura e precisão do código serão usados ​​para avaliar o desempenho do candidato. Portanto, certifique-se de que o código apresentado reflita o seu conhecimento tanto quanto possível!

Esperamos que uma solução possa ser alcançada dentro de um período de tempo razoável, considerando alguns dias, portanto, fique à vontade para usar o tempo da melhor forma possível. Entendemos que você pode ter uma agenda apertada, portanto, não hesite em nos contatar para qualquer solicitação adicional👍.

## Datasets

O que você precisará para completar este desafio está armazenado na pasta **data** deste repositório. Este diretório contém os seguintes arquivos: 

1. (:mag_right:) **Adult.data** (*Arquivo de dados*)
2. (:mag_right:) **Adult.test** (*Arquivo de dados*)
3. (:clipboard:) **Description** (*Arquivo de informações*)


## Enviando sua solução

Faça um fork deste projeto, e crie um branch com sua conta no Github, utilizando seu nome e sobrenome nele. Por exemplo, um branch com o nome *"Franklin Ferreira"* definirá que o candidato com o mesmo nome está fazendo o upload do código com a solução para o teste. Por favor, coloque os scripts e o código em pastas separadas (com o mesmo nome das pastas de arquivo fornecidas) para facilitar nossa análise.

Se desejar, crie um arquivo PDF com imagens nos indicando todo o processo que executou para gerar sua solução. Prezamos muito por bons *Storytellings*.

Além disso, esperamos que o candidato possa explicar o procedimento e a estratégia adotadas usando muitos, muitos e muitos comentários ou até mesmo um arquivo README separado. Esta parte da descrição é muito importante para facilitar nosso entendimento de sua solução! Lembre-se que o primeiro contato técnico com o candidato é por meio deste teste de codificação. Apesar de reforçarmos a importância da documentação e explicação do código, somos muito flexíveis para permitir a liberdade de escolher qual será o tipo de comunicação (por exemplo, arquivos README, comentários de código, etc).

Outra boa dica a seguir é o conceito geral de engenharia de software que também é avaliado neste teste. Espera-se que o candidato tenha um conhecimento sólido de tópicos como **Test-Driven Development (TDD)**, e paradigmas de código limpo em geral. Em resumo, é uma boa ideia prestar atenção tanto ao código quanto às habilidades dos engenheiros de software.

Depois de todas as análises e codificação serem feitas, crie uma solicitação de pull (PR) neste repositório.

# Resumo

Como uma ajuda extra, use a seguinte lista de verificação para se certificar de que todas as etapas do desafio foram concluídas:

- [ x ] Baixe todos os arquivos do teste neste repositório.
- [ x ] Crie uma solução adequada usando scripts, bibliotecas de código aberto, soluções de código próprio, etc. Considere que seguiremos suas instruções para executar seu código e ver o resultado.
- [ x ] Certifique-se de que a saída para o teste esteja de acordo com a saída necessária explicada aqui no arquivo *README.md*.
- [ x ] Se você está entusiasmado, pode nos enviar uma análise exploratória dos dados! :ok_hand:.
- [ x ] Faça comentários ou arquivos de documentação auxiliar (por exemplo, arquivos README) para auxiliar na interpretação de suas soluções. Lembre-se: adoramos ler seus comentários e explicações!
- [ x ] Salve o código resultante, scripts, documentação, etc. em pastas compatíveis com o mesmo nome do conjunto de dados de entrada (Apenas para nos ajudar! 👍)
- [ x ] Prepare os commits em branchs separados usando o padrão de nomeação: nome + sobrenome.
- [ x ] Envie o P.R.! (Dedos cruzados!:sunglasses:)
