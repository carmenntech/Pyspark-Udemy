# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType
from pyspark.sql.functions import col, regexp_extract

spark = SparkSession.builder.appName("Ejercicio2").getOrCreate()

df_users = spark.read.option("header", True).option("sep", ",").option("inferSchema", True).csv("dbfs:/FileStore/tables/users.csv")

df_photo = spark.read.option("header", True).option("sep", ",").option("inferSchema", True).csv("dbfs:/FileStore/tables/photos.csv")

df_likes = spark.read.option("header", True).option("sep", ",").option("inferSchema", True).csv("dbfs:/FileStore/tables/likes.csv")

df_users.dropDuplicates(["username"]).select("username").show()

df_likes.filter(df_likes.created_at.isNull()).show()
df_likes.na.fill("2000-01-01", subset=["created_at"]).show()

# INNER JOIN entre df_users y df_photo usando "user_id"
df_join1 = df_users.join(df_photo, df_users.id == df_photo.user_id)

# INNER JOIN entre el resultado anterior y df_likes usando "photo_id"
df_final = df_join1.join(df_likes, df_join1.user_id == df_likes.user_id)


df_anti_join = df_users.join(df_photo, df_users.id == df_photo.user_id, "left_anti")

