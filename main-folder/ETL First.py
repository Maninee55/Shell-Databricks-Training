# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/inputfiles/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/inputfiles/

# COMMAND ----------

1. PySpark(PYTHON+SPARK)
2. SQL

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/mnt/adlsshelldatabricks/raw/inputfiles/orders1.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Extract csv file from ADLS

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/adlsshelldatabricks/raw/inputfiles/orders1.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Transformation
# MAGIC add new column called current timestamp

# COMMAND ----------

from pyspark.sql.functions import *
df1= df.withColumn("ingestiondate",current_timestamp())

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df1

# COMMAND ----------

# MAGIC %sql
# MAGIC create database etl_first;

# COMMAND ----------

# MAGIC %sql
# MAGIC use etl_first;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Load the data into table
# MAGIC #### Write to table

# COMMAND ----------

df1.write.saveAsTable("etl_first.etl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_id,customer_id,order_date from etl 

# COMMAND ----------


