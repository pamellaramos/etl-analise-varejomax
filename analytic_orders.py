#!/usr/bin/env python
# coding: utf-8

# In[200]:


#importar bibliotecas 
get_ipython().run_line_magic('matplotlib', 'inline')
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
plt.style.use('ggplot')
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


# In[184]:


#importar o dados do bucket do cloud Storage 
df = pd.read_csv('gs://stg-gcp-interno/pamella-ramos/gold_orders/gold_orders.csv')


# In[186]:


#calculo da receita total e remoção de da coluna 
df['totalVend'] = df['unitPrice'] * df['unitsSold']
df.drop('totalRevenue', axis=1, inplace=True)
df = df.rename(columns = {'totalVend': 'totalRevenue'})


# In[189]:


#converção de datas
df['orderDate'] = pd.to_datetime(df['orderDate'], format='%d/%m/%Y')
df['shipDate'] = pd.to_datetime(df['shipDate'], format='%d/%m/%Y')


# In[190]:


#calculo do tempo entre a data da ordem e o despache
df['Days'] = df['shipDate'] - df['orderDate']


# In[194]:


df.describe()


# In[195]:


df['itemType'].value_counts().plot(kind='barh', figsize=(10,5), grid=False, rot=0, color='green')


# In[196]:


df['region'].value_counts().plot(kind='barh', figsize=(8,3), grid=False, rot=0, color='purple')


# In[197]:


df['SalesChannel'].value_counts().plot(kind='bar', figsize=(8,3), grid=False, rot=0, color='blue')


# In[198]:


df['orderPriority'].value_counts().plot(kind='bar', figsize=(8,3), grid=False, rot=0, color='blue')


# In[211]:


#conversão da data em integer
df['Days'] = df['Days'].astype(str)
df[['days', 'text']] = df['Days'].str.split(' ', expand=True)
df = df.drop('Days', axis=1)
df['days'] = df['days'].astype(int)


# In[223]:


# Salvar dados no GCS como um arquivo CSV
df.to_csv('gs://stg-gcp-interno/pamella-ramos/gold_orders2', index=False, header=True)

