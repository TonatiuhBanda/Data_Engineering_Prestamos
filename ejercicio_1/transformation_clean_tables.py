#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import *


# In[2]:


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


# ### Nombre archivos

# In[3]:


dir_archivo = '/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/ejercicios/'
dir_complemento = 'prestamos/stage/'


# In[4]:


nombre_archivos = [
    'prestamos_bancos.parquet',
    'prestamos_destinatarios.parquet',
    'prestamos_pagos.parquet',
    'prestamos_plaza.parquet',
    'prestamos_solicitudes.parquet',
    'prestamos_solicitudes_procesadas.parquet']


# ### Funciones

# In[5]:


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

    
letras_acento = 'áéíóúÁÉÍÓÚ'
letras_sin_acento = 'aeiouAEIOU'
tabla_acentos = str.maketrans(letras_acento, letras_sin_acento)


# ### Tabla prestamos_bancos

# In[6]:


nombre_archivo = nombre_archivos[0]
df_bancos = spark.read.format('parquet')                .load(dir_archivo+dir_complemento+nombre_archivo)

col_nombre = 'digitos'
df_bancos = df_bancos.withColumn(col_nombre, F.translate(col_nombre, "'", ""))
df_bancos = df_bancos.withColumnRenamed(col_nombre, col_nombre.upper())

col_nombre = 'BANCO'
df_bancos = df_bancos.withColumn(col_nombre, F.translate(col_nombre, "'", ""))

diccionario_banco = {
    'BAJ�O': 'BAJÍO',
    'VALU�': 'VALUÉ',
    '�NICA': 'ÚNICA',
    'CONSULTOR�A': 'CONSULTORÍA'}
for palabra in diccionario_banco.keys():
    df_bancos = df_bancos.withColumn('BANCO', F.regexp_replace('BANCO',
                                                               palabra,
                                                               diccionario_banco[palabra]))

df_bancos = df_bancos.withColumn(col_nombre,
                                 F.translate(col_nombre,
                                             letras_acento,
                                             letras_sin_acento))

df_bancos = df_bancos.withColumnRenamed(col_nombre, col_nombre.upper())


# In[7]:


df_bancos.printSchema()


# In[8]:


df_bancos.show(5)


# In[9]:


df_almacenamiento(dir_archivo, nombre_archivo, df_bancos)


# ### Tabla prestamos_destinatarios

# In[10]:


nombre_archivo = nombre_archivos[1]
df_destinatarios = spark.read.format('parquet')                        .load(dir_archivo+dir_complemento+nombre_archivo)


for col_nombre in ['ID', 'banco', 'plaza']:
    df_destinatarios = df_destinatarios.withColumn(col_nombre,
                                                   F.col(col_nombre).cast(IntegerType()))
    
    df_destinatarios = df_destinatarios.withColumnRenamed(col_nombre,
                                                          col_nombre.upper())

    
for col_nombre in ['cp', 'fecha_nacimiento', 'correo', 'clabe']:
    df_destinatarios = df_destinatarios.withColumnRenamed(col_nombre,
                                                          col_nombre.upper())


for col_nombre in ['nombre', 'direccion', 'empleo', 'empleador']:
    df_destinatarios = df_destinatarios.withColumn(col_nombre, 
                                                   F.translate(col_nombre,
                                                               letras_acento,
                                                               letras_sin_acento))
    
    df_destinatarios = df_destinatarios.withColumn(col_nombre,
                                                   F.translate(col_nombre, '\n', ''))
    
    df_destinatarios = df_destinatarios.withColumnRenamed(col_nombre,
                                                          col_nombre.upper())


# In[11]:


df_destinatarios.printSchema()


# In[12]:


df_destinatarios.show(1, vertical=True, truncate=False)


# In[13]:


df_almacenamiento(dir_archivo, nombre_archivo, df_destinatarios)


# ### Tabla pagos

# In[14]:


nombre_archivo = nombre_archivos[2]
df_pagos = spark.read.format('parquet')                .load(dir_archivo+dir_complemento+nombre_archivo)
df_pagos = df_pagos.select('fecha', 'cantidad', 'prestamo')


col_nombre = 'fecha'
df_pagos = df_pagos.withColumnRenamed(col_nombre,
                                      col_nombre.upper())


col_nombre = 'cantidad'
df_pagos = df_pagos.withColumn(col_nombre,
                               F.col(col_nombre).cast(DoubleType()))

df_pagos = df_pagos.withColumnRenamed(col_nombre,
                                      col_nombre.upper())


col_nombre = 'prestamo'
df_pagos = df_pagos.withColumn(col_nombre,
                               F.translate(col_nombre, '[]', ''))

df_pagos = df_pagos.withColumnRenamed(col_nombre,
                                      col_nombre.upper())


# In[15]:


df_pagos.printSchema()


# In[16]:


df_pagos.show(5)


# In[17]:


df_almacenamiento(dir_archivo, nombre_archivo, df_pagos)


# ### Tabla plaza

# In[18]:


nombre_archivo = nombre_archivos[3]
df_plaza = spark.read.format('parquet')                .load(dir_archivo+dir_complemento+nombre_archivo)

col_nombre = 'digitos'
df_plaza = df_plaza.withColumn(col_nombre,
                               F.translate(col_nombre, "'", ""))

df_plaza = df_plaza.withColumnRenamed(col_nombre,
                                      col_nombre.upper())

col_nombre = 'PLAZA'
df_plaza = df_plaza.withColumn(col_nombre,
                               F.translate(col_nombre, "'", ""))

df_plaza = df_plaza.withColumn(col_nombre, 
                               F.translate(col_nombre,
                                           letras_acento,
                                           letras_sin_acento))

df_plaza = df_plaza.withColumnRenamed(col_nombre,
                                      col_nombre.upper())


# In[19]:


df_plaza.printSchema()


# In[20]:


df_plaza.show(5)


# In[21]:


df_almacenamiento(dir_archivo, nombre_archivo, df_plaza)


# ### Tabla solicitudes

# In[22]:


nombre_archivo = nombre_archivos[4]
df_solicitudes = spark.read.format('parquet')                        .load(dir_archivo+dir_complemento+nombre_archivo)


for col_nombre in ['ID', 'fecha', 'fecha_in', 'fecha_lim',
                   'monto', 'taza', 'fecha_pago']:
    df_solicitudes = df_solicitudes.withColumnRenamed(col_nombre,
                                                      col_nombre.upper())

    
col_nombre = 'descripcion'
df_solicitudes = df_solicitudes.withColumn(col_nombre,
                                           F.col(col_nombre).cast(FloatType()))

df_solicitudes = df_solicitudes.withColumnRenamed(col_nombre,
                                                  col_nombre.upper())


for col_nombre in ['producto', 'solicitante']:
    df_solicitudes = df_solicitudes.withColumn(col_nombre, 
                                               F.translate(col_nombre,
                                                           letras_acento,
                                                           letras_sin_acento))
    
    df_solicitudes = df_solicitudes.withColumnRenamed(col_nombre,
                                                      col_nombre.upper())


# In[23]:


df_solicitudes.printSchema()


# In[24]:


df_solicitudes.show(2, vertical=True, truncate=False)


# In[25]:


df_almacenamiento(dir_archivo, nombre_archivo, df_solicitudes)


# ### Tabla solicitudes_procesadas

# In[26]:


nombre_archivo = nombre_archivos[5]
df_solicitudes_p = spark.read.format('parquet')                        .load(dir_archivo+dir_complemento+nombre_archivo)


for col_nombre in ['fecha_aprobado', 'fecha_solicitud', 'fecha_limite',
                   'monto_aprobado', 'solicitud_id', 'respuesta']:
    df_solicitudes_p = df_solicitudes_p.withColumnRenamed(col_nombre,
                                                          col_nombre.upper())


col_nombre = 'monto_solicitado'
df_solicitudes_p = df_solicitudes_p.withColumn(col_nombre,
                                               F.col(col_nombre).cast(DoubleType()))

df_solicitudes_p = df_solicitudes_p.withColumnRenamed(col_nombre,
                                                      col_nombre.upper())


# In[27]:


df_solicitudes_p.printSchema()


# In[28]:


df_solicitudes_p.show(2, vertical=True, truncate=False)


# In[29]:


df_almacenamiento(dir_archivo, nombre_archivo, df_solicitudes_p)

