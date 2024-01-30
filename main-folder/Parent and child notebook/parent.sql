-- Databricks notebook source
-- MAGIC %python
-- MAGIC data=([(1,'a',30),(2,'b',23)])
-- MAGIC schema="id int, name string, age int"
-- MAGIC df=spark.createDataFrame(data,schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------


