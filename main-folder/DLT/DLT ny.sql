-- Databricks notebook source
-- MAGIC %run ./widgets

-- COMMAND ----------

select * from csv.`/databricks-datasets/retail-org/customers/`

-- COMMAND ----------

select * from json.`/databricks-datasets/retail-org/sales_orders/`

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers
AS SELECT * FROM cloud_files("${path}customers/", "csv")

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sales_orders_raw
AS SELECT * FROM cloud_files("${path}sales_orders/", "json")

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv")

CREATE OR REFRESH STREAMING TABLE sales_orders_raw
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json")
