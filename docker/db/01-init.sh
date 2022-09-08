#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "airflow" --dbname "airflow" <<-EOSQL
  CREATE DATABASE db_pytest;
  GRANT ALL PRIVILEGES ON DATABASE db_pytest TO airflow;
  \connect db_pytest airflow
  BEGIN;
    CREATE TABLE IF NOT EXISTS public.dados (
    age INT NOT NULL,
    workclass char(50) NOT NULL,
    fnlwgt INT NOT NULL,
    education char(50) NOT NULL,
    education_num INT NOT NULL, 
    marital_status char(50) NOT NULL,
    occupation char(50) NOT NULL,
    relationship char(50) NOT NULL,
    race char(50) NOT NULL,
    sex char(15) NOT NULL,
    capital_gain INT NOT NULL,
    capital_loss INT NOT NULL,
    hours_per_week INT NOT NULL,
    native_country char(50) NOT NULL,
    class char(10) NOT NULL
	);
  COMMIT;
EOSQL