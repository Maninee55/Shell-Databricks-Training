-- Databricks notebook source
-- MAGIC %python
-- MAGIC Creating delta table 
-- MAGIC 1. SQL
-- MAGIC 2. Python
-- MAGIC 3. DF

-- COMMAND ----------

-- MAGIC %fs ls  dbfs:/mnt/adlsshelldatabricks/raw/delta/naval/people10m/_delta_log

-- COMMAND ----------

use naval;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS naval.people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
) location 'dbfs:/mnt/adlsshelldatabricks/raw/delta/naval/people10m'

-- COMMAND ----------

create schema shell location 'dbfs:/mnt/adlsshelldatabricks/raw/db/shell'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS shell.people20m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
)

-- COMMAND ----------

describe extended shell.people20m

-- COMMAND ----------

describe extended naval.people10m 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Internal of delta lake

-- COMMAND ----------

1. parquet files 
2. _delta_log
  - JSON Transaction log files
  - CRC- cyclic redundant check
  - parquet checkpoint file

-- COMMAND ----------

describe history naval.people10m 

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/mnt/adlsshelldatabricks/raw/delta/naval/people10m/_delta_log/00000000000000000000.json

-- COMMAND ----------

{"commitInfo":{"timestamp":1705050894166,"userId":"8901371287953883","userName":"shell1@techlanders.com","operation":"CREATE TABLE","operationParameters":{"partitionBy":"[]","description":null,"isManaged":"false","properties":"{}","statsOnLoad":false},"notebook":{"notebookId":"3407931326256123"},"clusterId":"0108-110130-7h1qxtew","isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{},"tags":{"restoresDeletedRows":"false"},"engineInfo":"Databricks-Runtime/13.3.x-photon-scala2.12","txnId":"de88707b-aeab-4104-a871-25343e3c9298"}}
{"metaData":{"id":"57882057-7a91-4339-8a1b-d75e69ba44d5","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"firstName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"middleName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"gender\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthDate\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ssn\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"salary\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1705050893778}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}


-- COMMAND ----------

Insert into naval.people10m (id, firstname, middlename, lastname) values (1, "Sachine","R", "Tendulkar")

-- COMMAND ----------

{"commitInfo":{"timestamp":1705052419806,"userId":"8901371287953883","userName":"shell1@techlanders.com","operation":"WRITE","operationParameters":{"mode":"Append","statsOnLoad":false,"partitionBy":"[]"},"notebook":{"notebookId":"3407931326256123"},"clusterId":"0108-110130-7h1qxtew","readVersion":0,"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"1","numOutputBytes":"1725"},"tags":{"restoresDeletedRows":"false"},"engineInfo":"Databricks-Runtime/13.3.x-photon-scala2.12","txnId":"957b91c2-6fec-4bca-b017-de5706675f25"}}
{"add":{"path":"part-00000-dcf13d90-ad4f-4b0a-b01e-f5c7f499a8d1.c000.snappy.parquet","partitionValues":{},"size":1725,"modificationTime":1705052419000,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"id\":1,\"firstName\":\"Sachine\",\"middleName\":\"R\",\"lastName\":\"Tendulkar\"},\"maxValues\":{\"id\":1,\"firstName\":\"Sachine\",\"middleName\":\"R\",\"lastName\":\"Tendulkar\"},\"nullCount\":{\"id\":0,\"firstName\":0,\"middleName\":0,\"lastName\":0,\"gender\":1,\"birthDate\":1,\"ssn\":1,\"salary\":1}}","tags":{"INSERTION_TIME":"1705052419000000","MIN_INSERTION_TIME":"1705052419000000","MAX_INSERTION_TIME":"1705052419000000","OPTIMIZE_TARGET_SIZE":"268435456"}}}


-- COMMAND ----------

select * from naval.people10m

-- COMMAND ----------

Insert into naval.people10m (id, firstname, middlename, lastname) values (1, "Sachine","R", "Tendulkar"),
(2, "Sachine","R", "Tendulkar"),
(3, "Sachine","R", "Tendulkar"),
(4, "Sachine","R", "Tendulkar")

-- COMMAND ----------

every transaction will create a log and parquet file

-- COMMAND ----------

select * from naval.people10m

-- COMMAND ----------

Insert into naval.people10m (id, firstname, middlename, lastname) values (9, "Sachine","R", "Tendulkar")

-- COMMAND ----------

delete from naval.people10m where id = 9 ;

-- COMMAND ----------

select * from naval.people10m

-- COMMAND ----------

delete from naval.people10m where id = 3;

-- COMMAND ----------

select * from naval.people10m

-- COMMAND ----------

describe history naval.people10m

-- COMMAND ----------



-- COMMAND ----------

delete from naval.people10m where id = 1;

-- COMMAND ----------

select * from naval.people10m

-- COMMAND ----------

update naval.people10m
set gender= 'M'
where id= 2;

-- COMMAND ----------

Create table naval.people10mv3 as
select * from naval.people10m version as of 3

-- COMMAND ----------

select * from naval.people10m timestamp as of '2024-01-12T09:52:00Z'

-- COMMAND ----------

delete from naval.people10m 

-- COMMAND ----------

select * from naval.people10m 

-- COMMAND ----------

describe history naval.people10m 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.table("naval.people10m")

-- COMMAND ----------



-- COMMAND ----------

Restore table naval.people10m to version as of 7

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.parquet("dbfs:/mnt/adlsshelldatabricks/raw/delta/naval/people10m/_delta_log/00000000000000000009.checkpoint.parquet"))

-- COMMAND ----------

if you delete parquet files you wont able to do time travel

-- COMMAND ----------

insert
insert
insert 
insert 
insert
1000 small small parquet files 

-- COMMAND ----------

Optimize in delta lake

-- COMMAND ----------

compact all small files into large file --- optimize (we can do time travel) zorder
Remove unsed parquet-- Vacuum (we CANNOT do time travel) - Retention peroid (7 days= 168 hours)


-- COMMAND ----------

describe history naval.people10m

-- COMMAND ----------

select * from naval.people10m

-- COMMAND ----------

optimize naval.people10m

-- COMMAND ----------

optimize naval.people10m
zorder by ('id')

-- COMMAND ----------

vacuum naval.people10m retain 240 hours

-- COMMAND ----------

vacuum naval.people10m retain 0 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum naval.people10m retain 0 hours dry run

-- COMMAND ----------

vacuum naval.people10m retain 0 hours

-- COMMAND ----------

describe history naval.people10m

-- COMMAND ----------

select * from naval.people10m version as of 2

-- COMMAND ----------


