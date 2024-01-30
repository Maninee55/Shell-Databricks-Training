-- Databricks notebook source
CREATE OR REPLACE STREAMING TABLE orders_bronze
COMMENT "order bronze table"
TBLPROPERTIES ("quality"="bronze")
AS SELECT current_timestamp() as processing_time, input_file_name() as source_file, * FROM cloud_files("${path}orders","csv", map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

CREATE STREAMING TABLE orders_silver
(CONSTRAINT valid_orderid EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW)
COMMENT "ALLOWING only valid orderid"
TBLPROPERTIES ("quality"="silver")
AS SELECT * EXCEPT(processing_time,source_file,_rescued_data ) FROM STREAM(LIVE.orders_bronze)

-- COMMAND ----------

CREATE LIVE TABLE count_product
as 
SELECT product, count(product) count, sum(quantity) total_quantity, sum(amount) total_amount FROM LIVE.orders_silver
GROUP BY product
