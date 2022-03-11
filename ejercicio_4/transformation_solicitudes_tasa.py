#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import *

from pyspark.sql.window import Window


# In[2]:


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


# ### Nombre archivos

# In[3]:


dir_archivo = '/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/ejercicios/'
dir_complemento = 'prestamos/curated/'


# ### Funciones

# In[4]:


def df_almacenamiento_parquet(dir_archivo, nombre_archivo, df):
    nombre_destino = f'prestamos/curated/{nombre_archivo}'
    df.write.mode('overwrite').parquet(dir_archivo+nombre_destino)
    print(nombre_destino)

    
def df_almacenamiento_csv(nombre_archivo, df):
    df_filtrado = df.limit(10)
    df_pandas = df_filtrado.toPandas()
    nombre_output = nombre_archivo.replace('.parquet', '')
    nombre_csv = f'output/{nombre_output}.csv'
    df_pandas.to_csv(nombre_csv, index=False)
    print(nombre_csv)


def df_almacenamiento(dir_archivo, nombre_archivo, df):
    df_almacenamiento_parquet(dir_archivo, nombre_archivo, df)
    df_almacenamiento_csv(nombre_archivo, df)


# ### Tabla prestamos_solicitudes

# In[5]:


nombre_archivo = 'prestamos_solicitudes.parquet'
df_solicitudes = spark.read.format('parquet')                        .load(dir_archivo+dir_complemento+nombre_archivo)

df_solicitudes = df_solicitudes.select(
    'PRODUCTO',
    F.col('TAZA').cast(FloatType()).alias('TASA')
)


# ### Tabla productos

# In[6]:


df_productos = df_solicitudes.groupBy('PRODUCTO')                             .agg(F.min('TASA').alias('MINIMO'),
                                  F.max('TASA').alias('MAXIMO'),
                                  F.avg('TASA').alias('PROMEDIO'))
df_productos.cache()


# In[7]:


df_productos.printSchema()


# In[8]:


df_productos.show(3, vertical=True, truncate=False)


# ### Almacenamiento

# In[9]:


nombre_archivo = 'prestamos_productos_tasa.parquet'
df_almacenamiento(dir_archivo, nombre_archivo, df_productos)


# ### Unpersist

# In[10]:


df_productos.unpersist()

