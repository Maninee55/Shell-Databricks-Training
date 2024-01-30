# Databricks notebook source
# MAGIC %md
# MAGIC https://learn.microsoft.com/en-gb/azure/databricks/connect/storage/tutorial-azure-storage

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get("shellkey","clientid")

# COMMAND ----------

container_name       = "raw"
storage_account_name = "adlsshelldatabricks"
client_id            = dbutils.secrets.get("shellkey","clientid")
tenant_id            = dbutils.secrets.get("shellkey","tenantid")
client_secret        = dbutils.secrets.get("shellkey","clientsecret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

for i in dbutils.secrets.get("shellkey","clientid"):
    print(i)

# COMMAND ----------


