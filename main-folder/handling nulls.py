# Databricks notebook source
schema=["name", "subject", "Marks", "Attendance"]

student_data=[("John","Math", 90, 80),("Michael", "Science", 70, None), ("David", "History", 50,40), ("Kelvin", "Computer", 40,None ),("Paul", "GEO", None, None), (None,None,  None, None),("John","Math", 90, 80),("John","Math", 90, 80),(None,None,  None, None),(None,None,  None, None),(None,None,  None, None),(None,"Math", 90, 80) ]

df=spark.createDataFrame(data=student_data, schema=schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df1=df.dropDuplicates()

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC - drop
# MAGIC - fill
# MAGIC - replace

# COMMAND ----------

help(df.dropna)

# COMMAND ----------

df1.dropna().display()

# COMMAND ----------

df2=df1.dropna("all")

# COMMAND ----------

df1.na.drop("all",subset=["Attendance"]).display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.fillna(50).display()

# COMMAND ----------

df2.fillna(50,subset="Attendance").display()

# COMMAND ----------

df2.fillna({"name":"unknown","Marks":0,"Attendance":25}).display()

# COMMAND ----------

df2.replace("Kelvin","kevin",subset="name").display()

# COMMAND ----------


