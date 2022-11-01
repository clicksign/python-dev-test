create table if not exists public.adult (
    id_adult            serial primary key,
    age                 bigint,
    workclass           varchar(50),
    fnlwgt              bigint,
    education           varchar(50),
    "education-num"     bigint,
    "marital-status"    varchar(50),
    occupation          varchar(100),
    relationship        varchar(50),
    race                varchar(50),
    sex                 varchar(6),
    "capital-gain"      real,
    "capital-loss"      real,
    "hours-per-week"    bigint,
    "native-country"    varchar(50),
    class               varchar(5),
    dat_import          timestamp default now()
)