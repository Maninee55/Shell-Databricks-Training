# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/complexjson.json`

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/complexjson.json")

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/complexjson.json")

# COMMAND ----------

df.display()

# COMMAND ----------

df1= df.withColumn("batters",explode("batters.batter"))\
    .withColumn("batters_id",col("batters.id"))\
        .withColumn("batters_type",col("batters.type"))\
            .drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
    .drop("topping")

# COMMAND ----------

df1.write.saveAsTable("naval.complexjson")

# COMMAND ----------

df1.write.saveAsTable("naval.complexjson")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table naval.complexjson

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.complexjson

# COMMAND ----------

df1.write.option("path","dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/complexjson").saveAsTable("naval.complexjsonext")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table naval.complexjsonext

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table complexjson as select * from delta.`dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/complexjson`

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table complexjson

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table complexjson location "dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/complexjson" as select * from delta.`dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/complexjson`

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table naval.complexjson location "dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/complexjson"

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail naval.complexjsonextpar

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended hive_metastore.naval.complexjsonext

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.complexjson

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail naval.complexjson;
# MAGIC describe extended naval.complexjson;
# MAGIC describe history  naval.complexjson

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended naval.complexjson

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history  naval.complexjson

# COMMAND ----------


