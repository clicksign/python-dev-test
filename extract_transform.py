"""
Este arquvo é responsável por realizar o processo de carregamento e tranformação inicial dos dados para que os mesmos
possam ser inseridos na base final.  Tal segmentação de processos em arquivos diferentes tem dois principais intuidos:
1 - Mimetizar os processos e pipelines de ETL reais, que muito comunmente  acontecem em diversos passos e não em um único script,
(devido volume, frequência de transação, necessidade de baixa latência e etc). Tais processos de ETL podem ser implementados
utilizando diferentes tecnologias e ferramentas como Serverless Functions, CDC (Change Data Capture em Databases), Airflow, NiFi,
Canais de Streamings e etc.

2 - Possibilitar a demonstração de várias alternativas para a inserção dos datasets na base final sem a necessidade de
duplicar o código de Load e Transformation ao mesmo tempo que se mantém a organização do repositório

"""

def run_extract_transform():
    import pandas as pd


    class DataFetcher:
        """
        Classe responsável por fazer o Fetch e agregamento das diferentes origens de dados para o inicio do processo
        de ETL. Apesar de ter sido implementada de forma estática (devido o desenho do desafio), tal classe pode ser
        expandida para trablhar dinamicamente diferentes origens de dados, parâmetros e etc
        """

        def __init__(self):
            self.columns = ['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation',
                            'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week',
                            'native-country',
                            'class']

        def fetch(self):
            df_adult_data = pd.read_csv('data/Adult.data', header=None, names=self.columns)
            df_adult_test = pd.read_csv('data/Adult.test', header=None, skiprows=1, names=self.columns)
            df = df_adult_data.append(df_adult_test)
            df.reset_index(drop=True, inplace=True)

            return df

    # Cada uma das Classes a seguir possui a responsabilidade única de tratar e tansformar cada um dos atributos da base
    # de forma individual, considerando as particularidades dos dados em cada um dos casos.
    # Esse modelo de arquitetura, embora implique em mais linhas de código, traz a possibilidade de atuar em um escopo reduzido,
    # garantindo assim a qualidade dos processos de transformação, a facilitando a realização de testes unitários e de integração.
    # Extrapolando o conceito, tal alquitetura, para além de tratar individualmente cada um dos atributos do um dataframe
    # de um problema específico, pode ser utilizada para absorver dados de origens diversas e com diferentes modelagens,
    # assegurando o escopo reduzido e organizado, auxiliando na manutenção de um código limpo e etc para um projeto mais complexo
    # Não obstante, tal arquitetura possui uma estrutura que facilita a implementação de versionamento do método de tratamento
    # para os diferentes Atributos/Fontes caso seja necessário, por exemplo, conviver com mais de uma versão de modelo de dados trafegantes



    class AttAge:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['age'] = df['age'].astype(str)
            df['age_is_numeric'] = df['age'].apply(lambda x: x.isnumeric())
            media_idades = df[df['age_is_numeric'] == True]['age'].astype(float).mean()
            df['age'] = df['age'].apply(lambda x: float(x) if x.isnumeric() else media_idades)
            df.drop('age_is_numeric', axis=1, inplace=True)
            return df


    class AttWorkClass:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['workclass'] = df['workclass'].apply(lambda x: x.replace(' ', ''))
            df['workclass'] = df['workclass'].apply(lambda x: None if x == '?' else x)
            return df


    class AttFnlWgt:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['fnlwgt'] = df['fnlwgt'].astype(str)
            df['fnlwgt'] = df['fnlwgt'].apply(lambda x: x.replace(' ', ''))
            df['fnlwgt_is_numeric'] = df['fnlwgt'].apply(lambda x: x.isnumeric())
            media_fnlwgt = df[df['fnlwgt_is_numeric'] == True]['fnlwgt'].astype(float).mean()
            df['fnlwgt'] = df['fnlwgt'].apply(lambda x: float(x) if x.isnumeric() else media_fnlwgt)
            df.drop('fnlwgt_is_numeric', axis=1, inplace=True)
            return df


    class AttEducation:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['education'] = df['education'].apply(lambda x: x.replace(' ', ''))
            return df


    class AttMartialStatus:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['marital-status'] = df['marital-status'].apply(lambda x: x.replace(' ', ''))
            return df


    class AttOccupation:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['occupation'] = df['occupation'].apply(lambda x: x.replace(' ', ''))
            df['occupation'] = df['occupation'].apply(lambda x: None if x == '?' else x)
            return df


    class AttRealtionship:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['relationship'] = df['relationship'].apply(lambda x: x.replace(' ', ''))
            return df


    class AttSex:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['sex'] = df['sex'].apply(lambda x: x.replace(' ', ''))
            return df

    class AttCapitalGain:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['capital-gain'] = df['capital-gain'].astype(str)
            df['capital-gain'] = df['capital-gain'].apply(lambda x: x.replace(' ', ''))
            df['capital-gain_is_numeric'] = df['capital-gain'].apply(lambda x: x.isnumeric())
            media_fnlwgt = df[df['capital-gain_is_numeric'] == True]['capital-gain'].astype(float).mean()
            df['capital-gain'] = df['capital-gain'].apply(lambda x: float(x) if x.isnumeric() else media_fnlwgt)
            df.drop('capital-gain_is_numeric', axis=1, inplace=True)
            return df

    class AttHoursPerWeek:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['hours-per-week'] = df['hours-per-week'].astype(str)
            df['hours-per-week'] = df['hours-per-week'].apply(lambda x: x.replace(' ', ''))
            df['hours-per-week_is_numeric'] = df['hours-per-week'].apply(lambda x: x.isnumeric())
            media_fnlwgt = df[df['hours-per-week_is_numeric'] == True]['hours-per-week'].astype(float).mean()
            df['hours-per-week'] = df['hours-per-week'].apply(
                lambda x: float(x) if x.isnumeric() else media_fnlwgt)
            df.drop('hours-per-week_is_numeric', axis=1, inplace=True)
            return df

    class AttNativeCountry:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['native-country'] = df['native-country'].astype(str)
            df['native-country'] = df['native-country'].apply(lambda x: x.replace(' ', ''))
            df['native-country'] = df['native-country'].apply(lambda x: None if x == '?' else x)
            return df

    class AttClass:
        def __init__(self):
            pass

        @staticmethod
        def corrigir(df):
            df['class'] = df['class'].apply(lambda x: x.replace(' ', '').replace('.', ''))
            return df

    print('Iniciando Transformação de Base')
    df = DataFetcher().fetch()
    df = AttAge().corrigir(df)
    df = AttWorkClass().corrigir(df)
    df = AttFnlWgt().corrigir(df)
    df = AttEducation().corrigir(df)
    df = AttMartialStatus().corrigir(df)
    df = AttOccupation().corrigir(df)
    df = AttRealtionship().corrigir(df)
    df = AttSex().corrigir(df)
    df = AttCapitalGain().corrigir(df)
    df = AttHoursPerWeek().corrigir(df)
    df = AttNativeCountry().corrigir(df)
    df = AttNativeCountry().corrigir(df)

    df.to_csv('outputs/base_tratada_para_inserir.csv',index=False)

    print("Transformação Realizada com sucesso")


