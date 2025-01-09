# Databricks notebook source
# MAGIC %md
# MAGIC Hay dos formas de crear un RDD:
# MAGIC
# MAGIC Paralelizando una colección ya existente en nuestra aplicación Spark.
# MAGIC
# MAGIC Referenciando un dataset de un sistema externo como HDFS, HBase, etc...

# COMMAND ----------

# PARALELIZACION

from pyspark.sql import SparkSession

rdd = spark.sparkContext.parallelize([1,2,3,4,5,6,7,8,9])

rdd.take(3)
#rdd.top(3)
#rdd.takeOrdered(3)
#rdd.collect()

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

#REFERENCIA A UN DATASET

rdd2 = spark.sparkContext.textFile("dbfs:/FileStore/tables/linkedinaperfilesmongo-2.csv")

# Verificar los primeros elementos del RDD
rdd2.take(5)

