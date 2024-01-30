-- Databricks notebook source
-- MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/inputdlt/customers/

-- COMMAND ----------

select * from csv.`dbfs:/mnt/adlsshelldatabricks/raw/inputdlt/customers/customers1.csv`

-- COMMAND ----------

CREATE STREAMING TABLE customers
AS SELECT current_timestamp() as processing_time, input_file_name() as source_files, * FROM cloud_files("dbfs:/mnt/adlsshelldatabricks/raw/inputdlt/customers/", "csv")

-- COMMAND ----------

Create streaming table customers_bronze_clean
(CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_name EXPECT (customer_name IS NOT NULL or operation="DELETE")) as 
select * from STREAM(LIVE.customers)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_silver;

APPLY CHANGES INTO LIVE.customers_silver
  FROM STREAM(LIVE.customers_bronze_clean)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY processing_time
  COLUMNS * EXCEPT (operation, source_file, _rescued_data)

-- COMMAND ----------



-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers
AS SELECT * FROM cloud_files("dbfs:/mnt/adlsshelldatabricks/raw/inputdlt/orders/", "csv")

-- COMMAND ----------


