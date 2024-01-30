# Databricks notebook source
inputfiles = "dbfs:/mnt/adlsshelldatabricks/raw/inputstreamfiles/json/"
output="dbfs:/mnt/adlsshelldatabricks/raw/outputstream"

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .load(f'{inputfiles}')
 .writeStream
 .option("checkpointLocation",f"{output}/naval/json/checkpoint")
 .option("path",f"{output}/naval/json/autoloader")
 .table("naval.autoloader")
 )

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation",f"{output}/naval/json/schema")
 .load(f'{inputfiles}')
 .writeStream
 .option("checkpointLocation",f"{output}/naval/json/checkpoint")
 .option("path",f"{output}/naval/json/autoloader")
 .table("naval.autoloader")
% )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.autoloader

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table naval.autoloader

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation",f"{output}/naval/json/schema")
 .option("cloudFiles.inferColumnTypes",True)
 .load(f'{inputfiles}')
 .writeStream
 .option("checkpointLocation",f"{output}/naval/json/checkpoint")
 .option("path",f"{output}/naval/json/autoloader")
 .table("naval.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.autoloader

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation",f"{output}/naval/json/schema")
 .option("cloudFiles.inferColumnTypes",True)
 .load(f'{inputfiles}')
 .writeStream
 .option("checkpointLocation",f"{output}/naval/json/checkpoint")
 .option("path",f"{output}/naval/json/autoloader")
 .option("mergeSchema",True)
 .table("naval.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.autoloader

# COMMAND ----------

Use case 2: Do not want to fail my stream

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation",f"{output}/naval/json/schema")
 .option("cloudFiles.inferColumnTypes",True)
 .option("cloudFiles.schemaEvolutionMode","rescue")
 .load(f'{inputfiles}')
 .writeStream
 .option("checkpointLocation",f"{output}/naval/json/checkpoint")
 .option("path",f"{output}/naval/json/autoloader")
 .option("mergeSchema",True)
 .table("naval.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.autoloader

# COMMAND ----------


