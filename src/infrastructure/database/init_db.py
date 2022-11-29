def create_table(engine):
    with engine.begin() as connection:

        sql_query = '''
        create table if not exists public.adult (
        id                  serial primary key,
        age                 int,
        workclass           varchar(50),
        fnlwgt              int,
        education           varchar(50),
        "education-num"     int,
        "marital-status"    varchar(50),
        occupation          varchar(100),
        relationship        varchar(50),
        race                varchar(50),
        sex                 varchar(6),
        "capital-gain"      int,
        "capital-loss"      int,
        "hours-per-week"    int,
        "native-country"    varchar(50),
        class               varchar(5)
        )
        '''

        connection.execute(sql_query)
