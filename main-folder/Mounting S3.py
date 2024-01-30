# Databricks notebook source
import urllib

# COMMAND ----------

access_key = "xxxxx"
secret_key = "xxxxx"
encoded_secret_key = urllib.parse.quote(secret_key,"")
aws_bucket_name = "nlys3"
mount_name = "aws_s3"

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/aws_s3/bronze

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from csv.`dbfs:/mnt/aws_s3/bronze/April.CSV`

# COMMAND ----------


