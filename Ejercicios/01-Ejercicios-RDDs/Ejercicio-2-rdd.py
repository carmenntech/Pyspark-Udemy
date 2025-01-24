# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark
import collections

# Crear una sesión de Spark
spark = SparkSession.builder.appName("Ejercicio_Ciudades").getOrCreate()

lines = spark.sparkContext.textFile("dbfs:/FileStore/tables/perfilesmongo-1.csv")

#results = lines.collect()
#print(results)

rdd_ciudades = lines.map(lambda x: (x.split(";")[2].strip(), 1))

header = rdd_ciudades.first()
rdd_ciudades_sin_header = rdd_ciudades.filter(lambda linea: linea != header)
ciudadesTotal = rdd_ciudades_sin_header.reduceByKey(lambda a,b: a+b)

results = ciudadesTotal.collect()
print(results)






