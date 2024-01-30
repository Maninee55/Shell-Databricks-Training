-- Databricks notebook source
https://learn.microsoft.com/en-us/azure/databricks/ingestion/copy-into/

-- COMMAND ----------

COPY INTO my_table
 FROM'/path/to/files'
 FILEFORMAT = CSV
 FORMAT_OPTIONS ('delimiter' = '|’,
 'header' = 'true')
 COPY_OPTIONS ('mergeSchema' = 'true’)

-- COMMAND ----------

select * from delta.`abfss://metadata@metastoredatabricksshell.dfs.core.windows.net/unitycatalog/0847ea65-ad50-4684-a0c4-2dd3b2ea27fc/tables/b58449d2-d30a-417a-8e7e-2f6eeb17efab`

-- COMMAND ----------

create table dev.naval.climatedata

-- COMMAND ----------

select * from dev.naval.climatedata

-- COMMAND ----------

describe extended dev.naval.climatedata

-- COMMAND ----------

abfss://metadata@metastoredatabricksshell.dfs.core.windows.net/unitycatalog/0847ea65-ad50-4684-a0c4-2dd3b2ea27fc/tables/6d6fa292-2542-44f8-b80f-d898a536fab5

-- COMMAND ----------

drop table dev.naval.climatedata

-- COMMAND ----------

COPY INTO dev.naval.climatedata
 FROM 'abfss://raw@adlsshelldatabricks.dfs.core.windows.net/climatedata'
 FILEFORMAT = CSV
 FORMAT_OPTIONS ('header' = 'true')
 COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

select * from dev.naval.climatedata

-- COMMAND ----------

select * from dev.naval.climatedata

-- COMMAND ----------


