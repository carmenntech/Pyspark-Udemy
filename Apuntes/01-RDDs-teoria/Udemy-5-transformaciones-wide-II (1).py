# Databricks notebook source
# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

Permite agrupar los datos a partir de una clave, repartiendo los resultados (shuffle) entre todos los nodos


# COMMAND ----------

from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AppGroupBy") \
    .getOrCreate()

rdd = sc.textFile("dbfs:/FileStore/tables/pdi_sales_small.csv")

# Creamos un RDD de pares con el nombre del país como clave, y una lista con los valores
paisesUnidades = rdd.map(lambda x: (x.split(";")[-1].strip(), x.split(";")))

# Quitamos el primer elemento que es el encabezado del CSV
header = paisesUnidades.first()
paisesUnidadesSinHeader = paisesUnidades.filter(lambda linea: linea != header)

# Agrupamos las ventas por nombre del país
paisesAgrupados = paisesUnidadesSinHeader.groupByKey()
paisesAgrupados.collect()

# COMMAND ----------

paisesAgrupadosLista = paisesAgrupados.map(lambda x: (x[0], list(x[1])))
paisesAgrupadosLista.collect()

# COMMAND ----------

from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AppGroupBy") \
    .getOrCreate()

rdd = sc.textFile("dbfs:/FileStore/tables/pdi_sales_small.csv")

# Creamos un RDD de pares con el nombre del país como clave, y una lista con los valores
paisesUnidades = rdd.map(lambda x: (x.split(";")[-1].strip(), x.split(";")))

# Quitamos el primer elemento que es el encabezado del CSV
header = paisesUnidades.first()
paisesUnidadesSinHeader = paisesUnidades.filter(lambda linea: linea != header)

# Agrupamos las ventas por nombre del país
paisesAgrupados = ventas.groupByKey()
paisesAgrupados.collect()
