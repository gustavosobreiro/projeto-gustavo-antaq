# -*- coding: utf-8 -*-
import os
import pyspark
import pyodbc
import numpy as np
import requests
import pandas as pd
from zipfile import ZipFile
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext


sc = SparkContext()
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()

#Scripts de extração em python com solução automatizada:

def baixar_arquivos(url, endereco):
    resposta = requests.get(url)
    if resposta.status_code == requests.codes.OK:
        with open(endereco, 'wb') as novo_arquivo:
            novo_arquivo.write(resposta.content)
        print("Download concluido. Salvo em: {}".format(endereco))
    else:
        resposta.raise_for_status()

if __name__ == "__main__":
    BASE_URL = 'http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/20{}.zip'
    OUTPUT_DIR = 'gustavo_github'
    for i in range(19, 22):
        nome_arquivo = os.path.join(OUTPUT_DIR, '20{}.zip'.format(i))
        baixar_arquivos(BASE_URL.format(i), nome_arquivo)


for i in range(19, 22):
    file_zip = '20{}.zip'
    z = ZipFile(file_zip.format(i),'r')
    z.extractall()
    z.close()

#Scripts de transformação em pyspark:

df_atracacao_2019 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2019Atracacao.txt")

df_atracacao_2020 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2020Atracacao.txt")

df_atracacao_2021 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2021Atracacao.txt")

df_atracacao_total = df_atracacao_2019.union(df_atracacao_2020.union(df_atracacao_2021))

#TEMPOS ATRACAÇÃO

df_tempos_atracacao_2019 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2019TemposAtracacao.txt")

df_tempos_atracacao_2020 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2020TemposAtracacao.txt")

df_tempos_atracacao_2021 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2021TemposAtracacao.txt")

df_tempos_atracacao_total = df_tempos_atracacao_2019.union(df_tempos_atracacao_2020.union(df_tempos_atracacao_2021))

#CARGAS

df_carga_2019 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2019Carga.txt")

df_carga_2020 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2020Carga.txt")

df_carga_2021 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2021Carga.txt")

df_carga_total = df_carga_2019.union(df_carga_2020.union(df_carga_2021))

#CONTEINERS

df_carga_conteinerizada_2019 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2019Carga_Conteinerizada.txt")

df_carga_conteinerizada_2020 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2020Carga_Conteinerizada.txt")

df_carga_conteinerizada_2021 = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("inferSchema", "true") \
    .csv("2021Carga_Conteinerizada.txt")

df_carga_conteinerizada_total = df_carga_conteinerizada_2019.union(df_carga_conteinerizada_2020.union(df_carga_conteinerizada_2021))


#JUNÇÕES


df_atracacao_fato = df_atracacao_total.join(df_tempos_atracacao_total, df_atracacao_total.IDAtracacao == df_tempos_atracacao_total.IDAtracacao, "Inner")

df_carga_fato_parcial = df_carga_total.join(df_carga_conteinerizada_total, df_carga_total.IDCarga == df_carga_conteinerizada_total.IDCarga, "leftouter")

df_atracacao_total_select = df_atracacao_total.select("IDAtracacao", "Ano", "Mes", "Porto Atracação", "SGUF",)

df_carga_fato_total = df_carga_fato_parcial.join(df_atracacao_total_select, df_carga_fato_parcial.IDAtracacao == df_atracacao_total_select.IDAtracacao, "Inner")

#CARREGAMENTO NO SQLSERVER

server = 'localhost' 
database = 'ANTAQ' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')
cursor = cnxn.cursor()

table_name = "dbo.atracacao_fato"

columns = ", ".join(df_atracacao_fato.columns)

values = '('+', '.join(['?']*len(df_atracacao_fato.columns))+')'
      
statement = "INSERT INTO "+table_name+" ("+columns+") VALUES "+values

for index, row in df_atracacao_fato.iterrows():
     cursor.execute(statement,
        row.IDAtracacao, \
        row.CDTUP, \
        row.IDBerco, \
        row.Berco, \
        row.Porto_Atracacao, \
        row.Apelido_Instalacao_Portuaria, \
        row.Complexo_Portuario, \
        row.Tipo_da_Autoridade_Portuaria, \
        row.Data_Atracacao, \
        row.Data_Chegada, \
        row.Data_Desatracacao, \
        row.Data_Inicio_Operacao, \
        row.Data_Termino_Operacao, \
        row.Ano, \
        row.Mes, \
        row.Tipo_de_Operacao, \
        row.Tipo_de_Navegacao_da_Atracacao, \
        row.Nacionalidade_do_Armador, \
        row.FlagMCOperacaoAtracacao, \
        row.Terminal, \
        row.Municipio, \
        row.UF, \
        row.SGUF, \
        row.Regiao_Geografica, \
        row.N_da_Capitania,
        row.N_do_IMO, \
        row.TEsperaAtracacao, \
        row.TEsperaInicioOp, \
        row.TOperacao, \
        row.TEsperaDesatracacao, \
        row.TAtracado, \
        row.TEstadia)
cnxn.commit()
cursor.close()