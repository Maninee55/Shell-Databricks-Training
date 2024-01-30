-- Databricks notebook source
CREATE OR REPLACE STREAMING TABLE customer_bronze
COMMENT "customers bronze table"
TBLPROPERTIES ("quality"="bronze")
AS SELECT current_timestamp() as processing_time, input_file_name() as source_file, * FROM cloud_files("${path}customers","csv", map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

CREATE STREAMING TABLE customers_clean
( 
CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT valid_email EXPECT (email IS NOT NULL),
CONSTRAINT valid_operation EXPECT(operation IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "ALLOWING only valid orderid"
TBLPROPERTIES ("quality"="silver")
AS SELECT * FROM STREAM(LIVE.customer_bronze)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customer_silver;

APPLY CHANGES INTO
  live.customer_silver
FROM
  stream(LIVE.customers_clean)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  processing_time
COLUMNS * EXCEPT
  (operation, _rescued_data,processing_time,source_file)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

CREATE LIVE TABLE customer_count_state
as 
SELECT state, count(*) as customer_count FROM LIVE.customer_silver
GROUP BY state

-- COMMAND ----------

CREATE LIVE VIEW order_customer
as 
select a.order_id, a.customer_id, b.customer_name, b.state
from LIVE.orders_silver a
inner join LIVE.customer_silver b
on a.customer_id=b.customer_id

-- COMMAND ----------


