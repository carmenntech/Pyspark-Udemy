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

rdd_ciudades = lines.map(lambda x: (x.split(";")[1].strip()))

header = rdd_ciudades.first()
rdd_ciudades_sin_header = rdd_ciudades.filter(lambda x: x not in [header, '"'])

rdd_ciudades1 = rdd_ciudades_sin_header.flatMap(lambda x: (x.split("| ")))
rdd_ciudades2 = rdd_ciudades1.map(lambda x: (x.strip()))
rdd_ciudades3 = rdd_ciudades2.filter(lambda x: x != '"')
rdd_ciudades4 = rdd_ciudades3.map(lambda x: (x , 1))

#results = rdd_ciudades3.collect()
#print(results)

#for element in rdd_ciudades.collect():
#    print(element)

ciudadesTotal = rdd_ciudades4.reduceByKey(lambda a,b: a+b)

skills_ordenados = ciudadesTotal.sortBy(lambda x: x[1]).collect()


for element in skills_ordenados:
    print(element)


