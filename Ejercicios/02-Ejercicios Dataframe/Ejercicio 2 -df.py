# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType
from pyspark.sql.functions import col, regexp_extract

spark = SparkSession.builder.appName("Ejercicio2").getOrCreate()

df_users = spark.read.option("header", True).option("sep", ",").option("inferSchema", True).csv("dbfs:/FileStore/users.csv")

df_photo = spark.read.option("header", True).option("sep", ",").option("inferSchema", True).csv("dbfs:/FileStore/photos.csv")

df_likes = spark.read.option("header", True).option("sep", ",").option("inferSchema", True).csv("dbfs:/FileStore/likes.csv")

df_users.dropDuplicates(["username"]).select("username").show()

df_likes.filter(df_likes.created_at.isNull()).show()
df_likes.na.fill("2000-01-01", subset=["created_at"]).show()


df_join1 = df_users.join(df_photo, df_users.id == df_photo.user_id)
df_join2 = df_users.join(df_likes, df_users.id == df_likes.user_id)
df_join1.show()
df_join2.show()

df_anti_join = df_users.join(df_photo, df_users.id == df_photo.user_id, "left_anti")
df_anti_join.show()
