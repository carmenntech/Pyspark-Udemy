# Databricks notebook source
# MAGIC %md
# MAGIC Permite filtrar los elementos que cumplen una condición mediante filter

# COMMAND ----------

rdd = sc.parallelize([1, 2, 3, 4, 5])
resultRDD = rdd.filter(lambda x: x%2==0)
resultRDD.collect()     # [2, 4] 

# COMMAND ----------

# MAGIC %md
# MAGIC Si utilizamos distinct eliminaremos los elementos repetidos

# COMMAND ----------

rdd = sc.parallelize([1,1,2,2,3,4,5])
resultRDD = rdd.distinct()
resultRDD.collect()     # [4, 1, 5, 2, 3]

# COMMAND ----------

# MAGIC %md
# MAGIC Mediante union unimos dos RDD en uno

# COMMAND ----------

rdd1 = sc.parallelize([1,2,3,4])
rdd2 = sc.parallelize([5,6,7,8])
resultRDD = rdd1.union(rdd2)
resultRDD.collect()     # [1, 2, 3, 4, 5, 6, 7, 8]

# COMMAND ----------

# MAGIC %md
# MAGIC Mediante intersection, obtendremos los elementos que tengan en común

# COMMAND ----------

rdd1 = sc.parallelize([1,2,3,4])
rdd2 = sc.parallelize([3,4,5,6])
resultRDD = rdd1.intersection(rdd2)
resultRDD.collect()     # [3, 4]

# COMMAND ----------

# MAGIC %md
# MAGIC Mediante subtract, obtendremos los elementos propios que no estén en el RDD recibido

# COMMAND ----------

rdd1 = sc.parallelize([1,2,3,4])
rdd2 = sc.parallelize([3,4,5,6])
resultRDD = rdd1.subtract(rdd2)
resultRDD.collect()     # [1, 2]

# COMMAND ----------

# MAGIC %md
# MAGIC
