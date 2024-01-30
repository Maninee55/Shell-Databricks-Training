# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/Baby_Names.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

newcolumns= ['Year', 'First_Name', 'County', 'Gender', 'Count']

# COMMAND ----------

df.toDF(*newcolumns).display()

# COMMAND ----------

df.withColumnRenamed("First Name", "first_name")\
.withColumnRenamed("Sex", "gender")\
.display()

# COMMAND ----------

df.filter(col("Year")==2007).display()

# COMMAND ----------

df.withColumn("current_data",current_date()).where(year(col("current_data"))==2024).display()

# COMMAND ----------

df.where("Year=2007").display()

# COMMAND ----------

df.sort("Year".desc()).display()

# COMMAND ----------

df.sort(col("Year")).display()

# COMMAND ----------

df.sort(col("Year").desc()).display()

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy("Year").count().orderBy(col("Year").desc()).display()

# COMMAND ----------

df.write.format("parquet").save("dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/babynameparquet")

# COMMAND ----------

df.write.format("delta").save("dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/babynamedelta")

# COMMAND ----------

df.write.save("dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/babynamedelta")

# COMMAND ----------



# COMMAND ----------

df.write.option("delta.columnMapping.mode","name").save("dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/babynamedelta")

# COMMAND ----------

name="Raju"
domain="DE"

# COMMAND ----------

print(f"I am {name} working as {domain}")

# COMMAND ----------

output="dbfs:/mnt/adlsshelldatabricks/raw/processed/"

# COMMAND ----------

df.write.format("parquet").partitionBy("Year").save(f"{output}naval/babynameparquetbyyear")

# COMMAND ----------

df.write.option("delta.columnMapping.mode","name").partitionBy("Year").save(f"{output}naval/babynamedeltabyyear")

# COMMAND ----------

df.write.format("parquet").partitionBy("Year","Sex").save(f"{output}naval/babynameparquetbyyear&gender")

# COMMAND ----------

df.write.mode("append").option("delta.columnMapping.mode","name").partitionBy("Year","Sex").save(f"{output}naval/babynamedeltabyyear")

# COMMAND ----------

df.write.saveAsTable("naval.babyname")

# COMMAND ----------

df.write.option("path","dbfs:/mnt/adlsshelldatabricks/raw/processed/naval/babynameext")saveAsTable("naval.babyname")
