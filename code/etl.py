#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import pandas as pd
import sqlite3
import os
import boto3
import json

import params
from datetime import datetime


def trata_coluna_string(coluna, dominio):
    """
    tratamento padrão para campos com tipo de dados string.
    1. remove espaços a esquerda e direita
    2. coloca a informação em maiúsculas
    3. verifica se faz parte do domínio
    :param coluna: fatia do dataframe com a coluna
    :param dominio: lista de dados admitidos nesse campo
    :return: mesma fatia do dataframe com a coluna e os dados tratados
    """
    coluna.fillna('', inplace=True)
    coluna = coluna.apply(lambda x: x.strip())
    coluna = coluna.apply(lambda x: x.upper())
    dominio = [x.upper() for x in dominio]
    coluna = coluna[coluna.isin(dominio)]

    return coluna


def trata_coluna_numerico(dfc, ix):
    """
    tratamento padrão para colunas com tipo de dados numérico
    1. força o dado para numérico. Dados texto serão excluídos.
    2. preenche os nulos com -2
    :param dfc: dataframe que será tratado
    :param ix: nome do campo numérico
    :return: fatia do dataframe com a coluna e os dados tratados
    """
    dfc[ix] = pd.to_numeric(dfc[ix], errors='coerce')
    dfc[ix].fillna(-2, inplace=True)

    return dfc[ix]


def verifica_arquivo_executado(github):
    """
    Verifica qual arquivo deve ser lido. Existe uma lista nos parâmetros com os arquivos e uma lista no banco com
    o que já foi totalmente carregado. Aqui pegamos a lista de parâmetros e excluímos o que já foi carregado.
    :param github: lista de arquivos origem
    :return: lista de arquivos que ainda não foi executado totalmente
    """
    try:
        sql = 'select arquivo from controle_carga'
        lista_arquivo = pd.read_sql_query(sql, conn)
        lista_arquivo = list(lista_arquivo['arquivo'])
    except Exception as e:
        lista_arquivo = list()

    # subtrai githubPath - lista_arquivos.
    # Fica somente o que está no githubPath e não está na lista de arquivos.
    arquivo = [value for value in github if value not in lista_arquivo]

    return arquivo


def acabou():
    # não tem dados, então mata a máquina EC2
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cmd = 'crontab -r'  # limpa o crontab
    print(agora, cmd)
    os.system(cmd)
    # copia o banco pro bucket S3
    s3.Bucket(params.bucket_name).upload_file(
        db_name, 'us_census_bureau.db')
    print(agora, 'transferindo o banco SQLite para o S3')
    # copia o log pro bucket S3
    print(agora, 'transferindo o log para o S3')
    s3.Bucket(params.bucket_name).upload_file(
        log_file, 'us_census_bureau.log')
    # desliga a máquina somente se não tem mais registro para buscar
    cmd = 'finaliza.sh'
    # os.system(cmd)


def main():
    global s3
    global db_name
    global log_file
    global conn

    # acessando o s3 para buscar os parâmetros de execução
    s3 = boto3.resource('s3')
    content_object = s3.Object(params.bucket_name, params.object_name)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    # parâmetros de execução
    # caminho dos arquivos de leitura no github
    githubPath = json_content['githubPath']
    # nome do banco de dados ex.: us-bureau.db
    db_name = json_content['db_name']
    # qtd de linhas a cada execução, 1630
    qtd_linhas_por_carga = json_content['qtd_linhas_por_carga']
    log_file = json_content['log_file']  # nome do arquivo de log

    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(agora, 'variáveis importadas')

    try:
        conn = sqlite3.connect(db_name)  # conexão com o banco
        # busca o maior indice na tabela para continuar de onde parou
        sql = 'select max(index_col) qtd from df'
        qtd = pd.read_sql_query(sql, conn)
        qtd = int(qtd['qtd'][0])
        agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(agora, sql, '>>>', qtd)
    except Exception as e:
        # se não funcionar é porque o banco não existe
        # ou deu algum erro na tabela e ele reinicia
        agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(agora, e)
        cmd = 'rm -rf {}'.format(db_name)  # remove o banco antigo se existir
        print(agora, cmd)
        os.system(cmd)
        cmd = 'touch {}'.format(db_name)  # cria um banco novo
        print(agora, cmd)
        os.system(cmd)
        conn = sqlite3.connect(db_name)  # faz a conexão, agora vai
        qtd = 0
        # se é a primeira vez executando então pega o primeiro arquivo da lista

    # faz a leitura do arquivo diretamente no github e aplica o cabeçalho
    gitFile = verifica_arquivo_executado(githubPath)
    if len(gitFile) > 0:
        theOne = gitFile[0]
        agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(agora, 'arquivo escolhido', theOne)
    else:
        # ========================================================================
        # Para executar local e não desligar a sua máquina comenta a linha abaixo
        acabou()
        exit()

    df = pd.read_csv(theOne,
                     names=[
                         'age', 'workclass', 'fnlwgt', 'education',
                         'education-num', 'marital-status', 'occupation',
                         'relationship', 'race', 'sex', 'capital-gain',
                         'capital-loss', 'hours-per-week', 'native-country',
                         'class'
                     ])
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(agora, 'dataframe criado')

    # cria uma coluna de indice para poder continuar de onde parou
    df['index_col'] = df.index
    ini = qtd + 1  # continua no próximo
    fim = ini + qtd_linhas_por_carga  # busca os 1630 registros
    df = df[ini:fim]  # fatia o dataframe

    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(agora, 'ini', ini, 'fim', fim)

    # domínio das dimensões conforme especificação
    dominios = {
        'workclass': [
            'Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov',
            'Local-gov', 'State-gov', 'Without-pay', 'Never-worked'
        ],
        'education': [
            'Bachelors', 'Some-college', '11th', 'HS-grad', 'Prof-school',
            'Assoc-acdm', 'Assoc-voc', '9th', '7th-8th', '12th', 'Masters',
            '1st-4th', '10th', 'Doctorate', '5th-6th', 'Preschool'
        ],
        'marital-status': [
            'Married-civ-spouse', 'Divorced', 'Never-married', 'Separated',
            'Widowed', 'Married-spouse-absent', 'Married-AF-spouse'
        ],
        'occupation': [
            'Tech-support', 'Craft-repair', 'Other-service', 'Sales',
            'Exec-managerial', 'Prof-specialty', 'Handlers-cleaners',
            'Machine-op-inspct', 'Adm-clerical', 'Farming-fishing',
            'Transport-moving', 'Priv-house-serv', 'Protective-serv',
            'Armed-Forces'
        ],
        'relationship': [
            'Wife', 'Own-child', 'Husband', 'Not-in-family', 'Other-relative',
            'Unmarried'
        ],
        'race':
        ['White', 'Asian-Pac-Islander', 'Amer-Indian-Eskimo', 'Other', 'Black'],
        'sex': ['Male', 'Female'],
        'native-country': [
            'United-States', 'Cambodia', 'England', 'Puerto-Rico', 'Canada',
            'Germany', 'Outlying-US(Guam-USVI-etc)', 'India', 'Japan', 'Greece',
            'South', 'China', 'Cuba', 'Iran', 'Honduras', 'Philippines', 'Italy',
            'Poland', 'Jamaica', 'Vietnam', 'Mexico', 'Portugal', 'Ireland',
            'France', 'Dominican-Republic', 'Laos', 'Ecuador', 'Taiwan', 'Haiti',
            'Columbia', 'Hungary', 'Guatemala', 'Nicaragua', 'Scotland',
            'Thailand', 'Yugoslavia', 'El-Salvador', 'Trinadad&Tobago', 'Peru',
            'Hong', 'Holand-Netherlands'
        ],
        'class': ['>50K.', '<=50K.']
    }

    # lista de campos numéricos no dataframe
    campos_numericos = [
        'age', 'education-num', 'capital-gain', 'capital-loss', 'hours-per-week'
    ]

    df = df[df.age.str.isnumeric()]  # transformando o campo age em numérico

    # tratando as colunas string
    for i, k in dominios.items():
        df[i] = trata_coluna_string(df[i], k)

    # tratando as colunas numéricas
    for i in campos_numericos:
        df[i] = trata_coluna_numerico(df, i)

    # tratamento especial
    c = {'>50K.': True, '<=50K.': False}
    df['class'] = df['class'].map(c)

    df.fillna('não identificado', inplace=True)

    qtd = len(df)
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(agora, 'Fim da transformação. Tamanho do dataframe final', qtd)

    # quando o código fizer sua última leitura o dataframe terá tamanho zero ou qtd = 0.
    # enquanto tiver registro ele executa mais uma vez
    # quando acabar os registros ele mata a máquina para evitar consumo
    if qtd > 0:  # tem dados no dataframe
        # salva no banco de dados
        df.to_sql('df', con=conn, index=False, if_exists='append')
        agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(agora, 'dados inseridos')
        print('='*70)  # faixa para arrumar o log
    else:
        # não tem dados no dataframe, próximo arquivo
        controle_carga = pd.DataFrame([theOne], columns=['arquivo'])
        controle_carga.to_sql('controle_carga', con=conn,
                              index=False, if_exists='append')
        agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(agora, 'acabou o arquivo', theOne)


if __name__ == '__main__':
    main()
