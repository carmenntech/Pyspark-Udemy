# Databricks notebook source
# MAGIC %md
# MAGIC Spark organiza los datos en particiones, considerándolas divisiones lógicas de los datos entre los nodos del clúster.

# COMMAND ----------

rdd = sc.parallelize([1,1,2,2,3,3,4,5])
rdd.getNumPartitions() # 4
rdd = sc.parallelize([1,1,2,2,3,3,4,5], 2)
rdd.getNumPartitions() # 2

rddE = sc.textFile("empleados.txt")
rddE.getNumPartitions() # 2
rddE = sc.textFile("empleados.txt", 3)
rddE.getNumPartitions() # 3

# COMMAND ----------

# MAGIC %md
# MAGIC MapPartitions:
# MAGIC
# MAGIC A diferencia de la transformación map que se invoca por cada elemento del RDD/DataSet, mapPartitions se llama por cada partición.

# COMMAND ----------

rdd = sc.parallelize([1,1,2,2,3,3,4,5], 2)

def f(iterator): yield sum(iterator)
resultadoRdd = rdd.mapPartitions(f)
resultadoRdd.collect()  # [6, 15]

resultadoRdd2 = rdd.mapPartitions(lambda iterator: [list(iterator)])
resultadoRdd2.collect() # [[1, 1, 2, 2], [3, 3, 4, 5]]

# COMMAND ----------

# MAGIC %md
# MAGIC Modificando las particiones
# MAGIC
# MAGIC Podemos modificar la cantidad de particiones mediante dos transformaciones wide: coalesce y repartition.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC coalesce
# MAGIC
# MAGIC Reducir el número de particiones en un RDD o DataFrame
# MAGIC
# MAGIC Optimizar el rendimiento al reducir particiones, especialmente después de operaciones como filter que pueden generar muchas particiones vacías o poco pobladas

# COMMAND ----------

rdd = sc.parallelize([1,1,2,2,3,3,4,5], 4)
rdd.getNumPartitions() # 4
rdd1p = rdd.coalesce(3)
rdd1p.getNumPartitions() # 3

# COMMAND ----------

# MAGIC %md
# MAGIC repartition
# MAGIC
# MAGIC Cambiar el número de particiones, ya sea para aumentarlas o reducirlas.
# MAGIC
# MAGIC Redistribuir datos de manera uniforme en un número específico de particiones. Realiza un "shuffle" completo de los datos entre particiones, lo que garantiza un equilibrio más uniforme.

# COMMAND ----------

rdd = sc.parallelize([1,1,2,2,3,3,4,5], 3)
rdd.getNumPartitions() # 3
rdd2p = rdd.repartition(2)
rdd2p.getNumPartitions() # 2
