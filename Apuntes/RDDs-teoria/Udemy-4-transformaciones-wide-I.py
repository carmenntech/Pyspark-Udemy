# Databricks notebook source
# MAGIC %md
# MAGIC Mediante la transformación reducedByKey los datos se calculan utilizando una función de reducción a partir de la clave

# COMMAND ----------

from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Contar Palabras") \
    .getOrCreate()

# Crear una sesión de Spark
spark = SparkSession.builder.appName("ContarPalabras").getOrCreate()

#Divide el codigo por lineas
lines = spark.sparkContext.textFile("dbfs:/FileStore/tables/palabras.txt")

rdd_split_hastag = lines.flatMap(lambda x: x.split(','))

# Crear pares (palabra, 1)
palabra_1 = rdd_split_hastag.map(lambda palabra: (palabra, 1))

# Contar las palabras (sumar los valores por clave)
conteo_palabras = palabra_1.reduceByKey(lambda x, y: x + y)

# Ordenar las palabras por frecuencia (opcional)
conteo_ordenado = conteo_palabras.sortBy(lambda x: x[1], ascending=False)

# Mostrar los resultados
conteo_ordenado.collect()  # Esto devuelve una lista de pares (palabra, conteo)

# Imprimir los resultados
for palabra, conteo in conteo_ordenado.collect():
    print(f"{palabra}: {conteo}")




# COMMAND ----------

# MAGIC %fs ls /FileStore/tables
