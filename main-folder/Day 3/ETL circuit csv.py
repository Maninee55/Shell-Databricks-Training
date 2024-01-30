# Databricks notebook source
dbutils.fs.ls("dbfs:/mnt/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/

# COMMAND ----------

# CSV
# Method 1: Using PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Read the csv (Extract)

# COMMAND ----------

df.spark.read

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv",header=True)

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv",header=True, inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2: Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrame Functions
# MAGIC - select
# MAGIC - alias
# MAGIC - withColumnRenamed
# MAGIC - withColumn
# MAGIC
# MAGIC
# MAGIC ### Functions
# MAGIC - col
# MAGIC - concat
# MAGIC - lit
# MAGIC - current_timestamp()

# COMMAND ----------

Spark is lazy evaluated: 

# COMMAND ----------

df.display()

# COMMAND ----------

select circuitId, location from df

# COMMAND ----------

df.select("circuitId", "location").display()

# COMMAND ----------

df.select("circuitId".alias("circuit_id"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select circuitID as circuit_id, * from tablename

# COMMAND ----------

df1=df.select(col("circuitId").alias("circuit_id"),"*")

# COMMAND ----------

df.display()

# COMMAND ----------

df1.display()

# COMMAND ----------

df.withColumnRenamed("circuitid","circuit_id").display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn("location",concat("location", "country")).display()

# COMMAND ----------

df.withColumn("newcolumn",concat("location"," ","country")).display()

# COMMAND ----------

df.withColumn("newcolumn",concat("location",lit(" "),"country")).display()

# COMMAND ----------

Task: 
get a new column as ingestiondate with currenttimestamp in it

# COMMAND ----------



# COMMAND ----------

df.withColumn("ingestiondate", current_timestamp())

# COMMAND ----------

df1=df.withColumn("ingestiondate", current_timestamp()).drop("url")

# COMMAND ----------

df1.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3: Load/ Write

# COMMAND ----------

csv (1 gb)

databricks (97%) (80-90%)

Parquet(17 mb )


# COMMAND ----------

1. FILE (csv,json, parquet, delta) where(adls, blob, s3,....)

2. Table

# COMMAND ----------

df1.write.parquet("dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema naval

# COMMAND ----------

df1.write.saveAsTable("naval.circuit")

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/bala.db/circuit/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/circuit`

# COMMAND ----------


