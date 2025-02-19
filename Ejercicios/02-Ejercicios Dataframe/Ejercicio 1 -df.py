# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType
from pyspark.sql.functions import col, regexp_extract

spark = SparkSession.builder.appName("Ejercicio1_").getOrCreate()

df_users = spark.read.option("header", True).option("sep", ",").option("inferSchema", True).csv("dbfs:/FileStore/tables/users.csv")

df_photo = spark.read.option("header", True).option("sep", ",").option("inferSchema", True).csv("dbfs:/FileStore/tables/photos.csv")

esquema = StructType([

    StructField("photo_id", IntegerType(),False),
    StructField("user_id", IntegerType(), False),
    StructField("created_dat", DateType(), False)
])

df_likes = spark.read.option("header", True).option("sep",",").schema(esquema).csv("dbfs:/FileStore/tables/likes.csv")

df_users = df_users.withColumn("created_at", df_users.created_at.cast(DateType()))
df_photo = df_photo.withColumn("created_dat", df_photo.created_dat.cast(DateType()))

df_users.show()
df_users.printSchema()



df_users = df_users.withColumnRenamed("created_at","fecha_creacion_user")

df_users.show()

df_users_filtro1 = df_users.filter(col("username").rlike(r'\d$'))

df_users_filtro2.show()

df_users_filtro2 = df_users.filter(col("fecha_creacion_user") < "2017-01-01")

df_users_filtro2.show()
