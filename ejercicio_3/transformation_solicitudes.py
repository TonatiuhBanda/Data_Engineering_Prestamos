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
    F.col('ID').cast(IntegerType()).alias('SOLICITUD_ID'),
    'PRODUCTO',
    F.col('TAZA').cast(FloatType()).alias('TASA'),
    'SOLICITANTE')

df_solicitudes = df_solicitudes.withColumn('nombre_split', F.split(F.col('SOLICITANTE'), ' '))
df_solicitudes = df_solicitudes.withColumn(
    'nombre_indicador',
    F.when(F.col('nombre_split').getItem(0) == 'Ing.',   2)
     .when(F.col('nombre_split').getItem(0) == 'Dr.',    2)
     .when(F.col('nombre_split').getItem(0) == 'Sr(a).', 2)
     .when(F.col('nombre_split').getItem(0) == 'Lic.',   2)
     .when(F.col('nombre_split').getItem(0) == 'Mtro.',  2)
    .otherwise(1))
df_solicitudes = df_solicitudes.withColumn('nombre_elementos', F.size(F.col('nombre_split')))
df_solicitudes = df_solicitudes.withColumn(
    'nombre_procesado',
    F.when(F.col('nombre_indicador') == 2,
           F.concat_ws(' ',
                       F.expr("slice(nombre_split, nombre_indicador, nombre_elementos)")))
    .otherwise(F.col('SOLICITANTE')))

df_solicitudes = df_solicitudes.select(
    'SOLICITUD_ID',
    'PRODUCTO',
    'TASA',
    F.col('nombre_procesado').alias('NOMBRE_SOLICITANTE') )


# In[6]:


df_solicitudes.printSchema()


# In[7]:


df_solicitudes.show(2, vertical=True, truncate=False)


# ### Tabla prestamos_solicitudes_procesadas

# In[8]:


nombre_archivo = 'prestamos_solicitudes_procesadas.parquet'
df_solicitudes_p = spark.read.format('parquet')                        .load(dir_archivo+dir_complemento+nombre_archivo)

df_solicitudes_p = df_solicitudes_p.select(
    F.to_date('FECHA_APROBADO', 'yyyy-MM-dd').alias('FECHA_APROBADO'),
    F.to_date('FECHA_SOLICITUD', 'yyyy-MM-dd').alias('FECHA_SOLICITUD'),
    F.to_date('FECHA_LIMITE', 'yyyy-MM-dd').alias('FECHA_LIMITE'),
    F.col('MONTO_SOLICITADO').cast(IntegerType()),
    F.col('MONTO_APROBADO').cast(FloatType()),
    F.col('SOLICITUD_ID').cast(IntegerType()))


# In[9]:


df_solicitudes_p.printSchema()


# In[10]:


df_solicitudes_p.show(2, vertical=True, truncate=False)


# ### Cruce tablas

# In[11]:


df_solicitudes_expandida = df_solicitudes_p.join(df_solicitudes, how='left', on=['SOLICITUD_ID'])

df_solicitudes_expandida = df_solicitudes_expandida.select(
    'FECHA_APROBADO',
    'FECHA_SOLICITUD',
    'FECHA_LIMITE',
    'MONTO_SOLICITADO',
    'MONTO_APROBADO',
    'SOLICITUD_ID',
    'PRODUCTO',
    'TASA',
    'NOMBRE_SOLICITANTE')

df_solicitudes_expandida.cache()


# In[12]:


df_solicitudes_expandida.printSchema()


# In[13]:


df_solicitudes_expandida.show(n=2, vertical=True, truncate=False)


# ### Almacenamiento

# In[14]:


nombre_archivo = 'prestamos_solicitudes_expandida.parquet'
df_almacenamiento(dir_archivo, nombre_archivo, df_solicitudes_expandida)


# ### Unpersist

# In[15]:


df_solicitudes_expandida.unpersist()

