#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


dir_archivo = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/datasets/"


# In[3]:


nombre_archivos = [
    'prestamos_bancos.csv',
    'prestamos_destinatarios.csv',
    'prestamos_pagos.csv',
    'prestamos_plaza.csv',
    'prestamos_solicitudes.csv',
    'prestamos_solicitudes_procesadas.csv']


# ### Tablas

# In[4]:


for nombre_archivo in nombre_archivos:
    if nombre_archivo == 'prestamos_plaza.csv':
        str_encoding = 'cp850'
    else:
        str_encoding = None
    df = pd.read_csv(dir_archivo+f'prestamos/{nombre_archivo}', encoding=str_encoding)
    
    dir_destino = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/ejercicios/"
    nombre_destino = f"prestamos/stage/{nombre_archivo.replace('.csv','')}.parquet"
    
    df.to_parquet(dir_destino+nombre_destino)

