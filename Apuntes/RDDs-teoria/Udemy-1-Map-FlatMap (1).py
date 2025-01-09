# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC TRANSFORMACIONES NARROW
# MAGIC
# MAGIC MAP()
# MAGIC
# MAGIC - map(func) transforma cada elemento del RDD de manera independiente, aplicando la función func a cada elemento.
# MAGIC
# MAGIC - El resultado es un nuevo RDD donde cada elemento de entrada se corresponde con exactamente un elemento de salida.
# MAGIC
# MAGIC - Es una transformación narrow, lo que significa que no implica un shuffle de datos entre particiones; todo el procesamiento ocurre dentro de la misma partición.

# COMMAND ----------

rdd = sc.parallelize([1, 2, 3, 4])  # Crear un RDD con los números 1, 2, 3, 4
resultado = rdd.map(lambda x: x * 2)  # Multiplicar cada elemento por 2
print(resultado.collect()) 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC FLATMAP()
# MAGIC
# MAGIC - flatMap(func) también aplica una función func a cada elemento del RDD. Sin embargo, a diferencia de map(), puede devolver cero, uno o múltiples elementos por cada elemento de entrada. toma cada elemento, lo transforma en varias partes (puede ser ninguna, una, o más de una), y luego aplana todo en una lista.
# MAGIC
# MAGIC
# MAGIC
# MAGIC - Los elementos resultantes se "aplanan" automáticamente, creando un RDD unidimensional (sin estructuras anidadas como listas de listas).

# COMMAND ----------

rdd = sc.parallelize(["a b c", "d e", "f"])  # Crear un RDD con cadenas
resultado = rdd.flatMap(lambda x: x.split(" "))  # Dividir cada cadena por espacio
print(resultado.collect())  # Salida: ['a', 'b', 'c', 'd', 'e', 'f']

# COMMAND ----------

# MAGIC %md
# MAGIC
