# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, when, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Crear una sesión de Spark
spark = SparkSession.builder.appName("Ejercicio_Ciudades").getOrCreate()

df_inferido = spark.read.option("header", True).option("sep",";").option("inferSchema", True).csv("dbfs:/FileStore/tables/perfilesmongo.csv")

df_inferido.printSchema()

df_inferido.show()

esquema = StructType([
    StructField("_id", StringType(), False),
    StructField("skills", StringType(), False),
    StructField("location", StringType(), False),
    StructField("numcontactos", StringType(), False)
])

df_esquema_creado = spark.read.option("header", True).schema(esquema).csv("dbfs:/FileStore/tables/perfilesmongo.csv")
df_esquema_creado.printSchema()

df_esquema_creado1 = df_esquema_creado.withColumn("numcontactos", df_esquema_creado.numcontactos.cast(IntegerType()))
df_esquema_creado1.printSchema()



df_inferido.withColumn("numcontactos2", (df_inferido.numcontactos + 10)).show()

df_inferido.withColumn("newcontactos2", (df_inferido.numcontactos - 10)).show()

df_inferido.withColumnRenamed("numcontactos", "numcontactos1").show()






