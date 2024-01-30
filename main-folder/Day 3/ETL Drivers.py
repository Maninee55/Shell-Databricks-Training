# Databricks notebook source
# MAGIC %sql
# MAGIC Create table jathin.drivers as 
# MAGIC select * from json.`dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/drivers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.jathin.drivers
