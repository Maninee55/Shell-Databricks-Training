-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/adlsshelldatabricks/raw/energydata/World Energy Consumption.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.columns

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=df.withColumn("ingestiondate",current_timestamp())

-- COMMAND ----------

use catalog dev;
create schema if not exists work;
use schema work

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1.write.option("mergeSchema",True).mode("overwrite").saveAsTable("dev.work.energy")

-- COMMAND ----------


