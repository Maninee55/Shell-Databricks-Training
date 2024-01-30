# Databricks notebook source
get path

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1

# COMMAND ----------

read json

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/constructors.json")

# COMMAND ----------

get new column called ingestion date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.withColumn("ingestiondate",current_timestamp())

# COMMAND ----------

write to a table in your schema. tablename(constructor)

# COMMAND ----------

df1.write.saveAsTable("naval.constructor")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table naval.constructors as 
# MAGIC select * from json.`dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,current_timestamp() as ingestiondate from hive_metastore.naval.constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table naval.drivers as 
# MAGIC select * from json.`dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/drivers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.naval.drivers

# COMMAND ----------


