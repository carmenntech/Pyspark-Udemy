# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC dbutils son utilidades para trabajar con archivos y almacenamiento de objetos de manera eficiente
# MAGIC
# MAGIC El término DBFS proviene de Databricks File System , 
# MAGIC que describe el sistema de archivos distribuido que utiliza Databricks para interactuar con el almacenamiento en la nube

# COMMAND ----------

#Listar los ficheros que hay en una ruta

dbutils.fs.ls("dbfs:/FileStore/tables")

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

#Crea una carpeta 

dbutils.fs.mkdirs("dbfs:/FicherosCursoPyspark")

# COMMAND ----------

#mover un archivo o directorio

dbutils.fs.mv ("dbfs:/FileStore/tables/linkedinaperfilesmongo-1.csv" , "dbfs:/FicherosCursoPyspark/linkedin.csv" )

# COMMAND ----------

# MAGIC %fs ls /FicherosCursoPyspark

# COMMAND ----------

#Elimina un archivo o directorio

dbutils.fs.rm("dbfs:/FicherosCursoPyspark/linkedin.csv")


# COMMAND ----------

# MAGIC %fs ls /FicherosCursoPyspark
