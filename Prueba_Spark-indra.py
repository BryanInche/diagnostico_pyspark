#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Inicializar SparkSession y SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

#Creamos la sesion en Spark
SpSession = SparkSession \
                .builder   \
                .appName("DBA - Spark Indra")  \
                .getOrCreate()
  
#Obtener el Spark Context del Spark Session
SpContext = SpSession.sparkContext    


# In[ ]:


inputPath="/FileStore/tables/Play_soccer_2.csv"  
autoData = SpContext.textFile(inputPath) # Genera el RDD
autoData.cache()


# Pregunta 1

import math
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

def transformTo(inputStr) :
  
  attList= inputStr.split(",")
  
  short_name = attList[2]
  long_name = attList[3]
  age = float(attList[4])
  height_cm = float(attList[6])
  weight_kg = float(attList[7])
  nationality = attList[8]
  club_name = attList[9]
  overall = float(attList[12])
  potential = float(attList[13])
  team_position = attList[26]
  
  #crear variables de indicadores 
  #clase = 1.0 if attList[41] == "normal" else 0.0
  
  
  
  #Crear una fila con los datos limpios y datos en buen formato
  values = Row(  short_name = short_name,    \
               long_name = long_name,   \
               age = age,            \
               height_cm = height_cm,  \
               weight_kg = weight_kg,    \
               nationality = nationality,  \
               club_name = club_name,       \
               overall = overall,           \
               potential = potential,        \
               team_position = team_position

                
                 )
  return values


# In[ ]:


#cambiar vector
AutoRows = autoData.map(transformTo)

AutoData_DF = SpSession.createDataFrame(AutoRows)


# In[ ]:


AutoData_DF.take(10)


# In[ ]:


# Pregunta 2

from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

def func(column1, column2):
    if column1 ==  'Germany':
        return "A"
    if column1 == 'Brazil':
        return "B"
    return "C"

func_udf = udf(func, StringType())
AutoData_DF = AutoData_DF.withColumn('new_column',func_udf(AutoData_DF['nationality'], AutoData_DF['team_position']))



# Pregunta 3

AutoData_DF = AutoData_DF.withColumn('potential_vs_overall', AutoData_DF.potential / AutoData_DF.overall)
AutoData_DF.take(5)







