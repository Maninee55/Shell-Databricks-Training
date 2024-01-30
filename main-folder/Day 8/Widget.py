# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.text("schema","",)

# COMMAND ----------

dbutils.widgets.remove("Schema")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.combobox(name="Schema",defaultValue="naval",choices=["naval","bala","indran"], label="Databases")

# COMMAND ----------

dbutils.widgets.get("Schema")

# COMMAND ----------

dbutils.widgets.multiselect(name="Schema2",defaultValue="naval",choices=["naval","bala","indran"], label="Databases2")

# COMMAND ----------

dbutils.widgets.get("Schema2")

# COMMAND ----------


