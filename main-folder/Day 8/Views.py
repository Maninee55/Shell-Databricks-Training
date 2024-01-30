# Databricks notebook source
# DBTITLE 1, 
Views: Virtual table
1. Standard View(Table) (CVAS) (SQL)
2. Temp View (SQL , PySpark) 
3. Global Temp View(SQL, PySpark)

Python or Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC Create view circuitukview as
# MAGIC select * from hive_metastore.naval.circuit where country="UK"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from circuitukview

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended circuitukview

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace TEMP VIEW circuitaustview as
# MAGIC select * from hive_metastore.naval.circuit where country="Australia"

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.circuitukview

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from circuitaustview

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace  global TEMP VIEW circuitmalatview as
# MAGIC select * from hive_metastore.naval.circuit where country="Malaysia"

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/constructors.json")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC df.write.saveAsTable("")

# COMMAND ----------

df.createOrReplaceTempView("conjson")

# COMMAND ----------

df.createOrReplaceGlobalTempView("constglobal")

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from conjson

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver as 
# MAGIC select *, current_timestamp() as ingestiondate, input_file_name() as path from conjson

# COMMAND ----------


