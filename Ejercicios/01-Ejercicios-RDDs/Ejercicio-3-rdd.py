# Databricks notebook source
from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder.appName("Ejercicio_Ciudades").getOrCreate()

lines = spark.sparkContext.textFile("dbfs:/FileStore/tables/perfilesmongo-1.csv")

#Divisiones por ; y |
rdd_ciudades = lines.map(lambda x: (x.split(";")[1].strip()))
rdd_ciudades1 = rdd_ciudades_sin_header.flatMap(lambda x: (x.split("|") and x.split('/')))

#Filtros
header = rdd_ciudades1.first()
rdd_ciudades_sin_header = rdd_ciudades1.filter(lambda x: x != header and '"' not in x and x.strip() != "")

#Poner 1 y sumar
rdd_ciudades4 = rdd_ciudades_sin_header.map(lambda x: (x.strip() , 1))
ciudadesTotal = rdd_ciudades4.reduceByKey(lambda a,b: a+b)

#Ordenar
skills_ordenados = ciudadesTotal.sortBy(lambda x: x[1])

#Mostrar
for element in skills_ordenados.collect():
    print(element)


