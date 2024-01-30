-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. using DataFrame

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.table("hive_metastore.naval.autoloader").write.saveAsTable("dev.naval.autoloader")

-- COMMAND ----------

2. using SQL

-- COMMAND ----------

Create table dev.naval.circuit as select * from hive_metastore.naval.circuit

-- COMMAND ----------

deep clone(table, metadata, file)
shallow clone(table)

-- COMMAND ----------

Create table dev.naval.complexjsonextpar as select * from hive_metastore.naval.complexjsonextpar

-- COMMAND ----------

Create table dev.naval.people10m as select * from hive_metastore.naval.people10m

-- COMMAND ----------

Create table dev.naval.constructors as select * from hive_metastore.naval.constructors

-- COMMAND ----------

sync schema dev.naval from hive_metastore.naval dry run

-- COMMAND ----------



-- COMMAND ----------

sync schema dev.naval from hive_metastore.naval

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.createDataFrame([(1,'a')]).write.option("path","/mnt/adlsshelldatabricks/raw/newexternal").saveAsTable("hive_metastore.naval.demoext")

-- COMMAND ----------

select * from hive_metastore.naval.demoext

-- COMMAND ----------

describe extended hive_metastore.naval.demoext

-- COMMAND ----------

sync table dev.naval.demoext from hive_metastore.naval.demoext dry run

-- COMMAND ----------

sync table dev.naval.demoext from hive_metastore.naval.demoext

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("abfss://raw@adlsshelldatabricks.dfs.core.windows.net/"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("abfss://raw@adlsshelldatabricks.dfs.core.windows.net/newexternal/"))

-- COMMAND ----------

select * from delta.`abfss://raw@adlsshelldatabricks.dfs.core.windows.net/newexternal/`

-- COMMAND ----------

use catalog dev;
create schema orders_cust_dlt;
use schema orders_cust_dlt

-- COMMAND ----------

sync schema dev.orders_cust_dlt from hive_metastore.orders_cust_dlt dry run

-- COMMAND ----------

sync schema dev.orders_cust_dlt from hive_metastore.orders_cust_dlt

-- COMMAND ----------


