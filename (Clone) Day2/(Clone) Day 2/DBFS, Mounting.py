# Databricks notebook source
https://docs.databricks.com/en/dbfs/index.html

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs 

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mounting
# MAGIC - 1. Access Key (entire container) - deprecated
# MAGIC - 2. SAS (Fine Grained Control)
# MAGIC - 3. Service Principal (entire container)
