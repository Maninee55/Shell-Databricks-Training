# Databricks notebook source
# MAGIC %md
# MAGIC ### Mounting
# MAGIC - 1. Access Key (entire container) - deprecated
# MAGIC - 2. SAS (Fine Grained Control)
# MAGIC - 3. Service Principal (entire container)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access Key

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://<containername>@<storageaccount>.blob.core.windows.net",
  mount_point = "/mnt/<storageaccount>/<containername>",
  extra_configs = {"fs.azure.account.key.<storageaccount>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://raw@adlsshelldatabricks.blob.core.windows.net",
  mount_point = "/mnt/adlsshelldatabricks/raw",
  extra_configs = {"fs.azure.account.key.adlsshelldatabricks.blob.core.windows.net":"tJ0eQCyuq1zgc1/2TeJG5+47ZJtnx8JEBt7Nlo07RaBQtXlIt3NWtRqogwYILj5MEgareqxQHhn4+ASt9tTD+Q=="})

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/inputfiles/

# COMMAND ----------

Raw files(csv, json, parquet, delta, orc, etc.....)
csv
delimiter: tab, |, , 
header= true or false

1. PySpark Dataframe style
2. Spark SQL style

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Querying the files directly (cannot use options)
# MAGIC select * from fileformat.`path`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv` 

# COMMAND ----------

df= spark.read.csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df= spark.read.option("header",True).csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df= spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv")

# COMMAND ----------

spark

# COMMAND ----------


