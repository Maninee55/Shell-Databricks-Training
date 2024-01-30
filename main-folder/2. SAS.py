# Databricks notebook source
# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#access-azure-data-lake-storage-gen2-or-blob-storage-using-a-sas-token

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key="<sas-token-key>"))

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adlsshelldatabricks.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adlsshelldatabricks.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adlsshelldatabricks.dfs.core.windows.net","?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-01-19T18:26:51Z&st=2024-01-19T10:26:51Z&spr=https&sig=vHwjEfzq4Ty%2B31vbCHDfwTBm0JGUEiU7XoxOwtYu6N4%3D")

# COMMAND ----------

spark.read.load("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")

dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")

# COMMAND ----------

dbutils.fs.ls("abfss://raw@adlsshelldatabricks.dfs.core.windows.net/jsonfiles/")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@adlsshelldatabricks.dfs.core.windows.net/jsonfiles/"))

# COMMAND ----------

df=spark.read.option("header",True).csv("abfss://raw@adlsshelldatabricks.dfs.core.windows.net/jsonfiles/circuits.csv")

# COMMAND ----------


