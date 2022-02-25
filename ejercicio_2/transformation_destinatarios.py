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


# ### Tabla prestamos_destinatarios

# In[5]:


nombre_archivo = 'prestamos_destinatarios.parquet'
df_destinatarios = spark.read.format('parquet')                        .load(dir_archivo+dir_complemento+nombre_archivo)


df_destinatarios = df_destinatarios.select(
    F.col('ID').alias('ID_SOLICITANTE'),
    F.col('NOMBRE').alias('NOMBRE_SOLICITANTE'),
    'DIRECCION',
    'CP',
    'FECHA_NACIMIENTO',
    'CORREO',
    F.col('BANCO').alias('ID_BANCO'),
    F.col('PLAZA').alias('ID_PLAZA'),
    'CLABE')


#NOMBRE_SOLICITANTE
df_destinatarios = df_destinatarios.withColumn('nombre_split', F.split(F.col('NOMBRE_SOLICITANTE'), ' '))
df_destinatarios = df_destinatarios.withColumn(
    'nombre_indicador',
    F.when(F.col('nombre_split').getItem(0) == 'Ing.',   2)
     .when(F.col('nombre_split').getItem(0) == 'Dr.',    2)
     .when(F.col('nombre_split').getItem(0) == 'Sr(a).', 2)
     .when(F.col('nombre_split').getItem(0) == 'Lic.',   2)
     .when(F.col('nombre_split').getItem(0) == 'Mtro.',  2)
    .otherwise(1))
df_destinatarios = df_destinatarios.withColumn('nombre_elementos', F.size(F.col('nombre_split')))
df_destinatarios = df_destinatarios.withColumn(
    'nombre_procesado',
    F.when(F.col('nombre_indicador') == 2,
           F.concat_ws(' ',
                       F.expr("slice(nombre_split, nombre_indicador, nombre_elementos)")))
    .otherwise(F.col('NOMBRE_SOLICITANTE')))

# df_destinatarios = df_destinatarios.withColumn('nombre_split_drop', F.expr("slice(nombre_split, nombre_indicador, nombre_elementos)"))
# df_destinatarios = df_destinatarios.withColumn('nombre_procesado', F.concat_ws(' ', 'nombre_split_drop'))


#ESTADO
df_destinatarios = df_destinatarios.withColumn('direccion_split', F.split(F.col('DIRECCION'), ','))
df_destinatarios = df_destinatarios.withColumn('numero_elementos', F.size(F.col('direccion_split')))
df_destinatarios = df_destinatarios.withColumn('estado', F.col('direccion_split').getItem(F.col('numero_elementos') - 1 ))
df_destinatarios = df_destinatarios.withColumn('estado', F.trim(F.col('estado')))

df_destinatarios = df_destinatarios.withColumn('direccion_split_drop', F.expr("slice(direccion_split, 1, numero_elementos-1)"))
df_destinatarios = df_destinatarios.withColumn('direccion_procesado', F.concat_ws(',', 'direccion_split_drop'))


# In[6]:


df_destinatarios.printSchema()


# In[7]:


df_destinatarios.show(3)


# ### Tabla prestamos_bancos

# In[8]:


nombre_archivo = 'prestamos_bancos.parquet'
df_bancos = spark.read.format('parquet')                .load(dir_archivo+dir_complemento+nombre_archivo)

df_bancos = df_bancos.select(
    F.col('DIGITOS').alias('ID_BANCO').cast(IntegerType()),
    F.col('BANCO').alias('NOMBRE_BANCO'))


# In[9]:


df_bancos.printSchema()


# In[10]:


df_bancos.show(3)


# ### Tabla plaza

# In[11]:


nombre_archivo = 'prestamos_plaza.parquet'
df_plaza = spark.read.format('parquet')                .load(dir_archivo+dir_complemento+nombre_archivo)

df_plaza = df_plaza.select(
    F.col('DIGITOS').alias('ID_PLAZA').cast(IntegerType()),
    F.col('PLAZA').alias('NOMBRE_PLAZA'))

############################################################
# Se agrega código temporal para hacer ID_PLAZA único
############################################################
w1 = Window.partitionBy('ID_PLAZA').orderBy('NOMBRE_PLAZA')
df_plaza = df_plaza            .withColumn('row', F.row_number().over(w1))            .filter(F.col('row') == 1).drop('row')


# In[12]:


df_plaza.printSchema()


# In[13]:


df_plaza.show(3)


# ### Cruce tablas

# In[14]:


df_destinatarios_b = df_destinatarios.join(df_bancos, how='left', on=['ID_BANCO'])
df_destinatarios_p = df_destinatarios_b.join(df_plaza, how='left', on=['ID_PLAZA'])


# In[15]:


df_destinatarios_p = df_destinatarios_p.select(
    'ID_SOLICITANTE',
    F.col('nombre_procesado').alias('NOMBRE_SOLICITANTE'),
    F.col('direccion_procesado').alias('DIRECCION'),
    F.col('estado').alias('ESTADO'),
    'CP',
    'FECHA_NACIMIENTO',
    'CORREO',
    'ID_PLAZA',
    'NOMBRE_PLAZA',
    'ID_BANCO',
    'NOMBRE_BANCO',
    'CLABE')

df_destinatarios_p.cache()


# In[16]:


df_destinatarios_p.printSchema()


# In[17]:


df_destinatarios_p.show(n=2, vertical=True, truncate=False)


# ### Almacenamiento

# In[18]:


nombre_archivo = 'prestamos_destinatarios_expandida.parquet'
df_almacenamiento(dir_archivo, nombre_archivo, df_destinatarios_p)

