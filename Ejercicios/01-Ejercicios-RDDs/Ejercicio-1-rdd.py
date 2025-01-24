# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark
import collections

# Crear una sesión de Spark
spark = SparkSession.builder.appName("Ejercicio_Hastag").getOrCreate()

lines = spark.sparkContext.textFile("dbfs:/FileStore/tables/Hastag.txt")

rdd_split_hastag = lines.flatMap(lambda x: x.split('#'))
rdd_reduce_hastag = rdd_split_hastag.countByValue()
sortedResults = collections.OrderedDict( sorted(rdd_reduce_hastag.items()))


# Recoger y mostrar los resultados
# results = rdd_split_hastag.collect()
# print(results)

for key, value in sortedResults.items():
    print("%s %i" % (key, value))

