# Databricks notebook source
""""
-Selecciona los post que hayan tenido likes
-Muestra la cantidad de usuarios que se han creado por año
-Muestra la suma de likes y conteo de post por cada usario 
-Muestra el DAG del programa, despues añade un cache al final del codigo y vuelve a mostrar el DAG
"""



from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType
from pyspark.sql.functions import col, regexp_extract, year, count 

spark = SparkSession.builder.appName("Ejercicio3").getOrCreate()

df_users = spark.read.option("header", True).option("sep", ",").option("inferSchema", True).csv("dbfs:/FileStore/tables/users.csv")

df_photo = spark.read.option("header", True).option("sep", ",").option("inferSchema", True).csv("dbfs:/FileStore/tables/photos.csv")

df_likes = spark.read.option("header", True).option("sep", ",").option("inferSchema", True).csv("dbfs:/FileStore/tables/likes.csv")

df_photo = df_photo.withColumnRenamed("id", "photo_id")

df_join_postlikes = df_photo.join(df_likes, df_likes.user_id == df_photo.user_id)

df_users.groupBy(year("created_at")).count().show()

# INNER JOIN entre df_users y df_photo usando "user_id"
df_join_left = df_users.join(df_photo, df_users.id == df_photo.user_id, "left")

# INNER JOIN entre el resultado anterior y df_likes usando "photo_id"
df_final_left = df_join_left.join(df_likes, df_join_left.user_id == df_likes.user_id, "left_anti")

df_likes_user = df_likes.groupBy("user_id").count().orderBy("count", ascending=False)

df_final2 = df_final_left.groupBy("username").count("photo_id")

df_likes_user.show()

df_final2.show()

#df_likes_user.cache()

#df_likes_user.explain("formatted")






