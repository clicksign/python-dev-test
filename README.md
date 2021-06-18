# Desafio - Dev Python
A script capable of collecting data from two data files, validating its content
and writing it into a SQLite database.

# Dependencies
Tested on Python 3.9+<br>
pandas library<br>
sqlalchemy library<br>
matplotlib library<br>
jinja2 library<br>

# Type
Read and write.

# Variables - Processing
All variables may found at `services.variables.py VARIABLES`.<br>
- `data_file_path` a string variable representing
  the data file path related to project folder.<br>
Sample: `"data_file_path": "data/Adult.data"`
  
- `test_file_path` a string variable representing
  the test file path related to project folder.<br>
Sample: `"test_file_path": "data/Adult.test"`
  
- `data_file_skip_row` an integer variable representing the number
  of first rows to avoid during data process for data file.<br>
Sample: `"data_file_skip_row": 0`
  
- `test_file_skip_row` an integer variable representing the number
  of first rows to avoid during data process for test file.<br>
Sample: `"test_file_skip_row": 1`
  
- `expected_number_of_columns` an integer variable representing the number
  of expected columns for data file and test file.<br>
Sample: `"expected_number_of_columns": 15`

- `expected_header` a list of strings representing the ordered column names
  for data file and test file.<br>
Sample: `"expected_header": ["age", "workclass", "fnlwgt", "education",
                        "education num", "marital status", "occupation",
                        "relationship", "race", "sex", "capital gain",
                        "capital loss", "hours per week", "native country",
                        "class"]`
  
- `expected_values_and_types` a dictionary of integers representing the column position for 
  data file and test file which values may be a list of strings representing the expected values,
  or a type representing the expected typing for data file and test file.<br>
Sample: `"expected_values_and_types": {
        0: int,
        1: ["Private", "Self-emp-not-inc", "Self-emp-inc",
            "Federal-gov", "Local-gov", "State-gov", "Without-pay",
            "Never-worked"],
        2: int,
        3: ["Bachelors", "Some-college", "11th", "HS-grad", "Prof-school",
            "Assoc-acdm", "Assoc-voc", "9th", "7th-8th", "12th", "Masters",
            "1st-4th", "10th", "Doctorate", "5th-6th", "Preschool"],
        4: int,
        5: ["Married-civ-spouse", "Divorced", "Never-married", "Separated",
            "Widowed", "Married-spouse-absent", "Married-AF-spouse"],
        6: ["Tech-support", "Craft-repair", "Other-service",
            "Sales", "Exec-managerial", "Prof-specialty",
            "Handlers-cleaners", "Machine-op-inspct", "Adm-clerical",
            "Farming-fishing", "Transport-moving", "Priv-house-serv",
            "Protective-serv", "Armed-Forces"],
        7: ["Wife", "Own-child", "Husband", "Not-in-family",
            "Other-relative", "Unmarried"],
        8: ["White", "Asian-Pac-Islander", "Amer-Indian-Eskimo",
            "Other", "Black"],
        9: ["Female", "Male"],
        10: int,
        11: int,
        12: int,
        13: ["United-States", "Cambodia", "England", "Puerto-Rico",
             "Canada", "Germany", "Outlying-US(Guam-USVI-etc)", "India",
             "Japan", "Greece", "South", "China", "Cuba", "Iran",
             "Honduras", "Philippines", "Italy", "Poland", "Jamaica",
             "Vietnam", "Mexico", "Portugal", "Ireland", "France",
             "Dominican-Republic", "Laos", "Ecuador", "Taiwan",
             "Haiti", "Columbia", "Hungary", "Guatemala", "Nicaragua",
             "Scotland", "Thailand", "Yugoslavia", "El-Salvador",
             "Trinadad&Tobago", "Peru", "Hong", "Holand-Netherlands"],
        14: [">50K.", "<=50K.", ">50K", "<=50K"],
    }`
  
- `known_wrong_elements` a list of strings representing the known wrong elements that may
  compromise the data file and/or test file.<br>
Sample: `"known_wrong_elements": ["?"]`

- `drop_duplicated` a boolean representing the duplicated rows dropping execution.<br>
Sample: `"drop_duplicated": True`

- `unwelcome_chars_and_words` a dictionary of strings representing the unwelcome characters or words
  which values are strings representing the correct case.<br>
Sample: `"unwelcome_chars_and_words": {
        "-": " ",
        "&": " ",
        "(": " ",
        ")": " ",
        ".": " ",
        ">50K": "Bigger",
        "<=50K": "Smaller",
    }`
  
- `number_of_threads` an integer variable representing the number
  of threads used to process data file and test file.<br>
Sample: `"number_of_threads": 10`

- `verbosity` a boolean representing the code verbosity.<br>
Sample: `"verbosity": False`

- `run_every_seconds` an integer variable representing the seconds delay
  proceeding with the next data process.<br>
Sample: `"run_every_seconds": 10`

- `processing_data_limit` an integer variable representing the number of rows
  to be processed every execution.<br>
Sample: `"processing_data_limit": 1630`
  
# Variables - Analysis
All variables may found at `services.variables.py VARIABLES`.<br>
- `columns_to_be_analysed` a list of strings representing the column names
  for data file and test file to be analysed.<br>
Sample: `"columns_to_be_analysed": ["age", "workclass", "education",
                                    "educationnum", "maritalstatus", "occupation",
                                    "relationship", "race", "sex", "capitalgain",
                                    "capitalloss", "hoursperweek", "nativecountry",
                                    "class"]`
  


- `analysis_relation` a list of lists representing the graph type, 
  being:
  - `two_grouped_bar` a two grouped bar graph. Must be a three
    length list containing two lists of strings representing
    the column name, and the row value; and a string
    representing the consideration column. The second
    value in the lists must exist in the column 
    represented by the first value.<br>
    Sample: `"analysis_relation": [
                ["two_grouped_bar",
                    [
                        ["sex", "Male"],
                        ["sex", "Female"],
                        "race",
                    ],
                    ],`
    - `pie` a pie graph. Must be a two length list 
    containing one list of strings representing
    the columns names; and None or a string
    representing the consideration value. If the
    second value is a string, it must exist 
    in the column represented by the first value
    in the list.<br>
    Sample 01: `"analysis_relation": [
                ["pie",
                 [
                    ["sex", "capitalloss"],
                    None,
                 ],
                 ],`      
    Sample 02: `"analysis_relation": [
                ["pie",
                 [
                    ["sex", "maritalstatus"],
                    "Male",
                 ],
                 ],`   


# Variables restrictions
Use `main.py -t` to rapidly verify all variables compliance.<br>
1) All variables key must exist in `variables.py VARIABLES`;
2) All variables must be fulfilled;
3) `data_file_path` value must be a string and result in
an existing path;
4) `test_file_path` value must be a string and result in
an existing path;
5) `data_file_skip_row` value must be neutral or a positive integer
   and given the skipped rows pandas must be able to parse data file;
6) `test_file_skip_row` value must be neutral or a positive integer
   and given the skipped rows pandas must be able to parse test file;
7) `expected_number_of_columns` value must be a positive integer
   and represent the number of columns of data file and test file;
8) `expected_header` value must be a list of strings
   and its length must represent the number of columns of data file and test file;
9) `expected_values_and_types` value must be a dictionary of integers keys
   of types or lists of strings values and its length must represent the number
   of columns of data file and test file;
an existing path;
10) `drop_duplicated` value must be a bool;
11) `verbosity` value must be a bool;
12) `run_every_seconds` value must be neutral or a positive integer;
13) `processing_data_limit` value must be a positive integer;
14) `unwelcome_chars_and_words` value must be a dictionary of strings keys
   of strings values;
15) `number_of_threads` value must be neutral or a positive integer;
16) `columns_to_be_analysed` value must be a list of strings
   and its content must exists in `expected_header`;    
17) `analysis_relation` value must be a list of:<br>
    - `two_grouped_bar` value and a list of two lists of 
      strings, and a string;      
    - `pie` value and a list of one list of 
      strings, and a string or None;
    
# Getting started
1) Install pandas library with `pip install pandas`;
2) Install sqlalchemy library with `pip install sqlalchemy`;
3) Install matplotlib library with `pip install matplotlib`;
4) Install jinja2 library with `pip install jinja2`;
5) Access `services.variables.py` to configure the script;
6) Perform a variables' compliance test with `py.exe main.py --test`;
7) You can start the script manually with `py.exe main.py --start`;
8) You can continue from where you left off with `py.exe main.py --proceed`;
9) You can process only the first rows of data with  `py.exe main.py --start --one-time`;
10) You can continue from where you left off running only once with `py.exe main.py --proceed --one-time`;
11) Perform a SQLite content analysis and HTML view creation with `py.exe main.py --analyse`.

`-t | --test` tests variables and other functions to process data.<br>
`-s | --start` erases the database and start from scratch. Running
based on `services.variables.py` until the end of the file or interruption.<br>
`-p | --proceed` continue where you left off. Running
based on `services.variables.py` until the end of the file or interruption.<br>
`-ot | --one-time` run the previously command only one time. <br>
`-a | --analyse` analyses SQLite content and create an HTML view. <br>
`main.py <-t | --test | -s | --start | -p | --proceed | -a | --analyse> [-ot | --one-time]`

# More information
Author(s): Bruno Lançoni<br>
License: GNU General Public License family<br>
version: 1.0.0

# Utilities
Threads in this script are useful only on 100.000+ rows process.<br>
edit. Elapsed time (s): value before enhancement -> value after enhancement

| Number of threads | Rows processed | Elapsed time (s) |
|:-----------------:|:--------------:|:----------------:|
|         1         |      1000      |    3.5 -> 2.8    |
|         10        |      1000      |    7.7 -> 3.0    |
|         15        |      1000      |   11.7 -> 2.9    |
|         1         |      10000     |   12.5 -> 11.0   |
|         10        |      10000     |   13.2 -> 11.6   |
|         15        |      10000     |   13.0 -> 11.9   |
|         1         |      50000     |   49.0 -> 48.5   |
|         10        |      50000     |   52.0 -> 50.5   |
|         15        |      50000     |   52.4 -> 49.9   |
|         1         |      100000    |  103.6 -> 97.4   |
|         10        |      100000    |  101.8 -> 98.9  |
|         15        |      100000    |  103.8 -> 99.7   |

---
# Descrição da prova

Este repositório possui um teste que visa avaliar sua curiosidade, seus conhecimentos em Python, análise e limpeza de dados, Storytelling e conceitos relacionados a processos ETL/ELT. O teste possui seu próprio conjunto de arquivos, parâmetros, instruções e estratégias para ser resolvido. Portanto, estude cada detalhe com sabedoria.

# US Census Bureau - Criação de um processo ETL/ELT

Sua tarefa é criar um processo ETL/ELT com agendamento que transporte dados úteis, presentes nos datasets fornecidos, para um banco de dados relacional. Os critérios para a execução deste desafio são:

1. Suas **únicas e exclusivas** fontes de dados devem ser os datasets fornecidos neste repositório;
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

- [ ] Baixe todos os arquivos do teste neste repositório.
- [ ] Crie uma solução adequada usando scripts, bibliotecas de código aberto, soluções de código próprio, etc. Considere que seguiremos suas instruções para executar seu código e ver o resultado.
- [ ] Certifique-se de que a saída para o teste esteja de acordo com a saída necessária explicada aqui no arquivo *README.md*.
- [ ] Se você está entusiasmado, pode nos enviar uma análise exploratória dos dados! :ok_hand:.
- [ ] Faça comentários ou arquivos de documentação auxiliar (por exemplo, arquivos README) para auxiliar na interpretação de suas soluções. Lembre-se: adoramos ler seus comentários e explicações!
- [ ] Salve o código resultante, scripts, documentação, etc. em pastas compatíveis com o mesmo nome do conjunto de dados de entrada (Apenas para nos ajudar! 👍)
- [ ] Prepare os commits em branchs separados usando o padrão de nomeação: nome + sobrenome.
- [ ] Envie o P.R.! (Dedos cruzados!:sunglasses:)
