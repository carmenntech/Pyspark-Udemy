# Databricks notebook source
from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder.appName("Ejercicio_Ciudades").getOrCreate()

lines = spark.sparkContext.textFile("dbfs:/FileStore/tables/perfilesmongo-1.csv")

#Divisiones por ; y |
rdd_skill = lines.map(lambda x: (x.split(";")[1].strip()))
rdd_skill_split = rdd_skill.flatMap(lambda x: (x.split("|") or x.split('/')))

#Filtros
header = rdd_skill_split.first()
rdd_skill_sin_header = rdd_skill_split.filter(lambda x: x != header and '"' not in x and x.strip() != "")

#Poner 1 y sumar
rdd_skill_map = rdd_skill_sin_header.map(lambda x: (x.strip() , 1))
skillTotal = rdd_skill_map.reduceByKey(lambda a,b: a+b)

#Ordenar
skillTotal_ordenados = skillTotal.sortBy(lambda x: x[1])

#Mostrar
for element in skillTotal_ordenados.collect():
    print(element)



