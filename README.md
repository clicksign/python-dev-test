# Desafio - Dev Python

A Proposta da Solução.

Os detalhes da solução estão descritos no pdf "A Proposta da Solução.pdf"

A resolução final deste projeto possui quatro arquivos principais, pensados de modo a ilustrar uma parte do horizonte de conhecimentos que compõe meu background profissional: 
- EDA.ipynb
Jupyter Notebook utilizado para realizar a análise exploratória de dados, elaboração dos processos de transformação dos atributos da base e desenvolvimento de dois modelos de machine learning e inferencia de Insights.
- extract_transform.py;
Arquivo responsável por aplicar as transformações definidas através do processo de EDA  na base de dados de forma produtizada
- first_method_full_load.py;
Arquivo que um método de inserção da base de dados tratada após as transformações feitas por extract_transform.py em um banco de dados Postgresql. A ideia central deste método é carregar todo o conjunto de dados na memória, segmentar a base e inserir cada um dos lotes na tabela final a cada X segundos. Este método considera que a base original dos dados transformados é estática.
- second_method_load_base_segmentada.py.
Arquivo que um método de inserção da base de dados tratada após as transformações feitas por extract_transform.py em um banco de dados Postgresql. A ideia central deste método é mimetizar um tráfego parcial (em lote) dos dados da base transformada, simulando uma área de staging. Tal método considera que a base de dados transformada pode ser dinâmica de modo a aceitar que novos registros possam ser adicionados (‘appendados’) na área de staging. Este foi o arquivo utilizado como método final da solução do problema proposto.

## Dependências
- Python 3.8+
- Faça o download e instale o Python.
- Execute o comando pip install -r requirements.txt
- Docker
  - Para executar o projeto basta seguir o pipeline de containerização do Docker (docker build; docker run) e setar as variáveis de ambiente

