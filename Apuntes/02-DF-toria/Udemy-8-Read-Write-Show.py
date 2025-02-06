# Databricks notebook source
#Crear un df a partir de un fichero (READ)

#CSV

dfCSV = spark.read.csv("datos.csv")
dfCSV = spark.read.csv("datos/*.csv")   # Una carpeta entera

dfCSV = spark.read.option("sep", ";").csv("datos.csv")
dfCSV = spark.read.option("header", "true").csv("datos.csv")
dfCSV = spark.read.option("header", True).option("inferSchema", True).csv("datos.csv")
dfCSV = spark.read.options(sep=";", header=True, inferSchema=True).csv("pdi_sales.csv")

dfCSV = spark.read.format("csv").load("datos.csv") 

#TXT

dfTXT = spark.read.text("datos.txt")
dfTXT = spark.read.option("wholetext", true).text("datos/")
dfTXT = spark.read.format("txt").load("datos.txt")

#JSON

dfJSON = spark.read.json("datos.json")
dfJSON = spark.read.format("json").load("datos.json")

#PARQUET

dfParquet = spark.read.parquet("datos.parquet")
dfParquet = spark.read.format("parquet").load("datos.parquet")

# COMMAND ----------

#Persistiendo diferentes formatos (WRITE)

#CSV
dfCSV.write.csv("datos.csv")
dfCSV.write.format("csv").save("datos.csv")
dfCSV.write.format("csv").mode("overwrite").save("datos.csv")

#TXT
dfTXT.write.text("datos.txt")
dfTXT.write.option("lineSep",";").text("datos.txt")
dfTXT.write.format("txt").save("datos.txt")

#JSON
dfJSON.write.json("datos.json")
dfJSON.write.format("json").save("datos.json")

#PARQUET
dfParquet.write.parquet("datos.parquet")
dfParquet.write.mode("overwrite").partitionBy("fecha").parquet("datos/")
dfParquet.write.format("parquet").save("datos.parquet")

# COMMAND ----------

df.show(2)
# +------+---------+------+------+
# |nombre|apellidos|ciudad|sueldo|
# +------+---------+------+------+
# | Aitor|  Medrano| Elche|  3000|
# | Pedro|    Casas| Elche|  4000|
# +------+---------+------+------+
df.show(truncate=False)
# +------+---------+----------+------+
# |nombre|apellidos|ciudad    |sueldo|
# +------+---------+----------+------+
# |Aitor |Medrano  |Elche     |3000  |
# |Pedro |Casas    |Elche     |4000  |
# |Laura |García   |Elche     |5000  |
# |Miguel|Ruiz     |Torrellano|6000  |
# |Isabel|Guillén  |Alicante  |7000  |
# +------+---------+----------+------+
df.show(2, vertical=True)
# -RECORD 0------------
#  nombre    | Aitor   
#  apellidos | Medrano 
#  ciudad    | Elche   
#  sueldo    | 3000    
# -RECORD 1------------
#  nombre    | Pedro   
#  apellidos | Casas   
#  ciudad    | Elche   
#  sueldo    | 4000  

# COMMAND ----------

df.first()
# Row(nombre='Aitor', apellidos='Medrano', ciudad='Elche', sueldo=3000)
df.head()
# Row(nombre='Aitor', apellidos='Medrano', ciudad='Elche', sueldo=3000)
df.head(3)
# [Row(nombre='Aitor', apellidos='Medrano', ciudad='Elche', sueldo=3000),
#  Row(nombre='Pedro', apellidos='Casas', ciudad='Elche', sueldo=4000),
#  Row(nombre='Laura', apellidos='García', ciudad='Elche', sueldo=5000)]

nom1 = df.first()[0]           # 'Aitor'
nom2 = df.first()["nombre"]    # 'Aitor'

df.collect()
# [Row(nombre='Aitor', apellidos='Medrano', ciudad='Elche', sueldo=3000),
#  Row(nombre='Pedro', apellidos='Casas', ciudad='Elche', sueldo=4000),
#  Row(nombre='Laura', apellidos='García', ciudad='Elche', sueldo=5000),
#  Row(nombre='Miguel', apellidos='Ruiz', ciudad='Torrellano', sueldo=6000),
#  Row(nombre='Isabel', apellidos='Guillén', ciudad='Alicante', sueldo=7000)]
df.take(2)
# [Row(nombre='Aitor', apellidos='Medrano', ciudad='Elche', sueldo=3000),
#  Row(nombre='Pedro', apellidos='Casas', ciudad='Elche', sueldo=4000)]
nom = df.collect()[0][0]   
