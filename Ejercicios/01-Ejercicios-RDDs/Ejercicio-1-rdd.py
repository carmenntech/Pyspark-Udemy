# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ejercicio1").getOrCreate()

lines = spark.sparkContext.textFile("dbfs:/FileStore/tables/Hastag.txt")

rdd_split = lines.flatMap(lambda x: x.split('#'))

header = rdd_split.first()
rdd_split_sin_header = rdd_split.filter(lambda x: x != header)

rdd_reduce = rdd_split_sin_header.countByValue()

#for liness in rdd_reduce_hastag.collect():
#    print(liness)

for key, value in rdd_reduce.items():
    print("%s %i" % (key, value))
