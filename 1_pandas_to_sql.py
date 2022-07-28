import time

if __name__ == "__main__":
    from sqlalchemy import create_engine
    from sqlalchemy.types import VARCHAR,Numeric
    from tqdm import tqdm
    import pandas as pd

    engine = create_engine(
        'postgresql://user:passwd@host.com:5432/db_name',
        max_overflow=5,
        connect_args={"application_name": "ETL_TEST"},
        pool_size=30,
        pool_timeout=60
    )


    def segmentar_lotes_de_insercao(df, slice_len):

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


    df = pd.read_csv('outputs/base_tratada_para_inserir.csv')

    chunks = segmentar_lotes_de_insercao(df, 1630)

    dtypes = {
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

    for chunk in tqdm(chunks):
        df_slice = df.iloc[chunk]
        with engine.connect() as conn:
            df_slice.to_sql('tb_adults', con=conn,
                            schema='public', if_exists='append',
                            dtype=dtypes,
                            index=False)
        time.sleep(10)

