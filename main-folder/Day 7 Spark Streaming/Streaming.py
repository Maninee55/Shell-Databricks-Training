# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/inputstreamfiles/csv/

# COMMAND ----------

Spark Streaming
Limitation 
1. Schema must be specified
2. CDC data cannot be captured
Databricks
Autoloader (Spark Streaming)
Delta Live Table

batch
read
df=spark.read.csv("path")
write
df.write.save("path")

Stream
read
df=spark.readStream.csv("path")
write
df.writeStream.option("checkpointLoction","path")save("path")

# COMMAND ----------

df=spark.readStream.option("header",True).csv("dbfs:/mnt/adlsshelldatabricks/raw/inputstreamfiles/csv/")

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

Id,Name,Gender,Salary,Country,Date

# COMMAND ----------

users_schema= StructType([StructField("Id", IntegerType()),
                          StructField("Name", StringType()),
                          StructField("Gender", StringType()),
                          StructField("Salary", IntegerType()),
                          StructField("Country", StringType()),
                          StructField("Date", StringType()),
                          ])

# COMMAND ----------

input="dbfs:/mnt/adlsshelldatabricks/raw/inputstreamfiles/csv/"

# COMMAND ----------

df=spark.readStream.option("header",True).schema(users_schema).csv(f"{input}")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("ingestiondate",current_timestamp())

# COMMAND ----------

checkpoint="dbfs:/mnt/adlsshelldatabricks/raw/outputstream"

# COMMAND ----------

df1.writeStream.option("checkpointLocation",f"{checkpoint}/naval/checkpoint").table("naval.firststream")

# COMMAND ----------



# COMMAND ----------

(spark.
 readStream
 .option("header",True)
 .schema(users_schema)
 .csv(f"{input}")
 .withColumn("ingestiondate",current_timestamp())
    .writeStream
    .option("checkpointLocation",f"{checkpoint}/naval/checkpoint")
    .table("naval.firststream"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.firststream

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

spark.streams.active.
