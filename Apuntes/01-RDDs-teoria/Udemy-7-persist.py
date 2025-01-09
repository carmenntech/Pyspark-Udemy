# Databricks notebook source
# MAGIC %md
# MAGIC Por defecto, cuando ejecutas una acción (como count, collect, etc.) en un RDD o DataFrame, Spark recalcula todos los pasos anteriores de la transformación necesaria para llegar al resultado. Esto significa que si haces varias acciones en el mismo RDD/DataFrame, los pasos intermedios se repiten, lo cual puede ser ineficiente
# MAGIC
# MAGIC persist: Permite almacenar un RDD o DataFrame en memoria o disco (en diferentes niveles de almacenamiento) para evitar recalcularlo.
# MAGIC
# MAGIC cache: Es un caso especial de persist, donde el almacenamiento predeterminado es memoria.

# COMMAND ----------

from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder.appName("CacheExample").getOrCreate()

# Crear un DataFrame de ejemplo
data = [(1, "Alice"), (2, "Bob"), (3, "Cathy"), (4, "David")]
columns = ["id", "name"]
df = spark.createDataFrame(data, columns)

# Cachear el DataFrame
df.cache()

# Primera acción (se ejecutan las transformaciones y los datos se cachean)
print("Primera acción (count):")
print(df.count())

# Segunda acción (los datos ya están cacheados, es más rápido)
print("Segunda acción (show):")
df.show()


# COMMAND ----------

from pyspark import StorageLevel

# Persistir el DataFrame en memoria y disco
df.persist(StorageLevel.MEMORY_AND_DISK)

# Primera acción (almacena en memoria/disco tras ejecutar las transformaciones)
print("Primera acción (count):")
print(df.count())

# Segunda acción (lee del almacenamiento persistente)
print("Segunda acción (show):")
df.show()

# Liberar memoria/disco cuando ya no es necesario
df.unpersist()

