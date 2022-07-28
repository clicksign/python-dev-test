"""
Este método compõe

"""

if __name__ == "__main__":
    from sqlalchemy import create_engine
    from sqlalchemy.types import VARCHAR, Numeric
    from tqdm import tqdm
    import pandas as pd
    import time

    DB_USER = 'user'
    DB_PASS = 'passwd'
    DB_HOST = 'host.com'
    DB_PORT = 5432
    DB_NAME = 'db_name'
    DB_CONN = f'postgresql://{DB_USER}:@{DB_HOST}:{DB_PORT}/{DB_NAME}'


    class InsercaoAdults:

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

        def insert_in_database(self, chunks, time_to_sleep):
            for chunk in tqdm(chunks):
                df_slice = df.iloc[chunk]
                with self.engine.connect() as conn:
                    df_slice.to_sql('tb_adults', con=conn,
                                    schema='public', if_exists='append',
                                    dtype=self.dtypes,
                                    index=False)
                time.sleep(time_to_sleep)

        def run_insert(self, df, slice_len, time_to_sleep):
            chunks = self._segmentar_lotes_de_insercao(df, slice_len=1630)
            self.insert_in_database(chunks, time_to_sleep=10)

#
    df = pd.read_csv('outputs/base_tratada_para_inserir.csv')

    obj_insertor = InsercaoAdults()
    obj_insertor.run_insert(df, slice_len=1630, time_to_sleep=10)

