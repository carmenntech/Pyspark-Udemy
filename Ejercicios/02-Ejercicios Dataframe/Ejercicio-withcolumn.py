# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = SparkSession.builder.appName("Ejercicio_withCOlumn").getOrCreate()

df_inferido = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv("dbfs:/FileStore/tables/perfilesmongo.csv")

#df_inferido.show()
#df_inferido.printSchema()

esquema = StructType([

    StructField("_id", StringType(),False),
    StructField("skills", StringType(), False),
    StructField("location", StringType(), False),
    StructField("numcontactos", StringType(), False)
])

df_esquema_creado = spark.read.option("header", True).option("sep",";").schema(esquema).csv("dbfs:/FileStore/tables/perfilesmongo.csv")

#df_esquema_creado.show()
#df_esquema_creado.printSchema()

df_esquema_creado = df_esquema_creado.withColumn("numcontactos", df_esquema_creado.numcontactos.cast(IntegerType()))

#df_esquema_creado.printSchema()

df_inferido1 = df_inferido.withColumn("numcontactos2", (df_inferido.numcontactos + 10)).show()

df_esquema_creado.withColumnRenamed("numcontactos", "numcontactos1").show()
