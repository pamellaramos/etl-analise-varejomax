#!/usr/bin/env python
# coding: utf-8

# In[8]:


#Limpeza e tratamento dos dados


# In[9]:


#importação de bibliotecas
from google.cloud import storage
import pyspark
from pandas import DataFrame
import pyspark.sql.utils
from pyspark.sql.types import IntegerType, BooleanType, TimestampType, FloatType
from pyspark.sql.functions import regexp_replace, to_date, col, lit, create_map
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.cloud import bigquery


# In[10]:


#carregar os dados do bucket e criar um dataframe
df_bronze = spark.read.format("csv").load("gs://stg-gcp-interno/pamella-ramos/", 
                    sep =';', #delimitador é em ponto e vírgula
                    inferSchema=True, #percorre todo o arquivo e adpta automáticamente seu esquema
                    header=True)


# In[11]:


#remover espaços do cabeçalho das colunas
df_bronze = (df_bronze.withColumnRenamed("Region", "region")
        .withColumnRenamed("Country","country")
        .withColumnRenamed("Item Type","itemType")
        .withColumnRenamed("Sales Channel","salesChannel")
        .withColumnRenamed("Order Priority","orderPriority")
        .withColumnRenamed("Order Date","orderDate")
        .withColumnRenamed("Order ID","orderId")
        .withColumnRenamed("Ship Date","shipDate")
        .withColumnRenamed("Units Sold","unitsSold")
        .withColumnRenamed("Unit Price","unitPrice")
        .withColumnRenamed("Unit Cost","unitCost")
        .withColumnRenamed("Total Revenue","totalRevenue")
        .withColumnRenamed("Total Cost","totalCost")
        .withColumnRenamed("Total Profit","totalProfit"))


# In[12]:


#alterar de virgula para ponto e assim, aplicar todas as transformações dos tipos de dados
df_bronze = df_bronze.withColumn("unitPrice", regexp_replace("unitPrice",",","."))        .withColumn("unitCost", regexp_replace("unitCost",",","."))        .withColumn("totalRevenue", regexp_replace("unitPrice",",","."))        .withColumn("totalCost", regexp_replace("totalCost",",","."))        .withColumn("totalProfit", regexp_replace("totalProfit",",","."))        .withColumn("unitPrice", col("unitPrice").cast(FloatType()))        .withColumn("unitCost", col("unitCost").cast(FloatType()))        .withColumn("totalRevenue", col("totalRevenue").cast(FloatType()))        .withColumn("totalCost", col("totalCost").cast(FloatType()))        .withColumn("totalProfit", col("totalProfit").cast(FloatType()))


# In[13]:


#selecionar as colunas necessárias 
df_silver = df_bronze.select ('region',
                 'Country',
                 'itemType',
                 'SalesChannel',
                 'orderPriority', 
                 'orderDate',
                 'shipDate',
                 'unitsSold',
                 'unitPrice',
                 'unitCost',
                 'totalRevenue',
                 'totalCost',
                 'totalProfit')


# In[17]:



df_silver.summary("count").show()
print(df_silver.count()) #Fazendo a contagem de linhas


# In[18]:


# Salvar dados no GCS como um arquivo CSV
df_silver.write.csv('gs://stg-gcp-interno/pamella-ramos/gold_orders', header=True)


# In[ ]:




