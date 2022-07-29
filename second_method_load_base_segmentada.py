"""
Este método compõe

"""
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, Numeric
from tqdm import tqdm
import pandas as pd
import time
from first_method_full_load import InsercaoAdults
from environment_variables import DB_CONN


class StageData():

    def read_staging_data_last_index(self):
        """
        Esta função é responsável por ler qual o ultimo indice trafegado da tabela. O intuito é utilizá-lo para carregar
        em memória um lote limitada de informações, não tendo necessidade de carregar a base inteira para a memória.
        Obviamente, o arquivo em texto funciona como um simulacro de uma tabela do banco de dados, a fim de demonstrar
        horizontes de conhecimento. Uma consulta realizada uma tabela Postgres de banco de dados real pode ser realizada
        através do Engine Connection do SQLAlchemy ou conexão de Cursor do pacote Psycopg2, seguida da execução de query
        da tabela.
        :return:
        """
        with open('outputs/staging_index_load.txt', 'r') as f:
            index = f.read()
        if len(index) == 0:
            index = 0
        else:
            index = int(index)
        return index
    def atualizar_indice(self,novo_indice):
        with open('outputs/staging_index_load.txt', 'w') as f:
            f.write(f'{novo_indice}')

    def get_slice_para_inserir(self, index_ultima_execucao, df, slice_len):

        df_indices = list(df.index.values)

        if index_ultima_execucao + slice_len < len(df_indices):
            staged_data = df.iloc[index_ultima_execucao:index_ultima_execucao + slice_len]
            index_final = index_ultima_execucao + slice_len
        if index_ultima_execucao + slice_len >= len(df_indices):
            staged_data = df.iloc[index_ultima_execucao:]
            index_final = index_ultima_execucao + len(staged_data)
        return index_final, staged_data

    def select_stage_data_to_insert(self, slice_len):
        full_staged_data = pd.read_csv('outputs/base_tratada_para_inserir.csv')
        index_ultima_execucao = self.read_staging_data_last_index()
        index_atualizado, slice_para_inserir = self.get_slice_para_inserir(index_ultima_execucao, full_staged_data,
                                                                           slice_len)
        return index_atualizado, slice_para_inserir


class InsercaoStagedAdults(InsercaoAdults, StageData):

    def __init__(self):
        self.engine = create_engine(DB_CONN,
                                    connect_args={"application_name": "ETL_TEST"})
        self.dtypes = {
            'age': Numeric(),
            'workclass': VARCHAR(),
            'fnlwgt': Numeric(),
            'education': VARCHAR(),
            'education-num': Numeric(),
            'marital-status': VARCHAR(),
            'occupation': VARCHAR(),
            'relationship': VARCHAR(),
            'race': VARCHAR(),
            'sex': VARCHAR(),
            'capital-gain': Numeric(),
            'capital-loss': Numeric(),
            'hours-per-week': Numeric(),
            'native-country': VARCHAR(),
            'class': VARCHAR()
        }

    @staticmethod
    def _segmentar_lotes_de_insercao(df, slice_len):

        chunks = []
        slice_len = slice_len
        indices = list(df.index.values)
        i = 0
        while i < len(indices):
            if i + slice_len < len(indices):
                chunks.append(indices[i:i + slice_len])
            else:
                chunks.append(indices[i:len(indices)])
            i += slice_len
        return chunks

    def insert_in_database(self, df):
        print("Inserindo em Database")
        try:
            with self.engine.connect() as conn:
                df.to_sql('tb_adults', con=conn,
                          schema='publc', if_exists='append',
                          dtype=self.dtypes,
                          index=False)
        except Exception as ex:
            print(f'Erro ao inserir no Banco de Dados>>> {ex}')
            raise ex

    def run_insert(self, slice_len):
        index_atualizado, slice_para_inserir = self.select_stage_data_to_insert(slice_len)
        self.insert_in_database(slice_para_inserir)
        self.atualizar_indice(index_atualizado)
        print(index_atualizado)


#
def run_insert_db_final(slice_len):
    obj_insertor = InsercaoStagedAdults()
    obj_insertor.run_insert(slice_len=slice_len)
    return True


# if __name__ == "__main__":
#     while True:
#         run_insert_db_final(slice_len=1630)
#         time.sleep(10)
#         print("Insercao de lote concluida")
