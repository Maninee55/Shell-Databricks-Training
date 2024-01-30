# Databricks notebook source
from delta.tables import *
DeltaTable.createOrReplace(spark)\
    .tableName("naval.emp")\
    .addColumn("emp_id","INT")\
    .addColumn("emp_name","STRING")\
    .addColumn("gender","STRING")\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.emp

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into naval.emp values(1,"Sachin","M")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import * 
employee_data= [(2,'Virat','M')]
schema= StructType([StructField("emp_id",IntegerType()),
                    StructField("emp_name",StringType()),
                    StructField("gender",StringType())
                   ])

df=spark.createDataFrame(employee_data,schema)
display(df)

# COMMAND ----------

df.write.saveAsTable("naval.emp")

# COMMAND ----------

df.write.mode("append").saveAsTable("naval.emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.emp

# COMMAND ----------

employee_data1= [(3,'Dhoni','M',"Batsman")]
schema1= StructType([StructField("emp_id",IntegerType()),
                    StructField("emp_name",StringType()),
                    StructField("gender",StringType()),
                    StructField("Dept",StringType())
                   ])

df1=spark.createDataFrame(employee_data1,schema1)
display(df1)

# COMMAND ----------

df1.write.mode("append").saveAsTable("naval.emp")

# COMMAND ----------

df1.write.option("mergeSchema", "true").mode("append").saveAsTable("naval.emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.emp

# COMMAND ----------

employee_data1= [(4,'Shumban','M',"Batsman")]
schema1= StructType([StructField("employee_id",IntegerType()),
                    StructField("employee_name",StringType()),
                    StructField("gender",StringType()),
                    StructField("Department",StringType())
                   ])

df2=spark.createDataFrame(employee_data1,schema1)
display(df2)

# COMMAND ----------

df2.write.mode("append").saveAsTable("naval.emp")

# COMMAND ----------

df2.write.option("mergeSchema", "true").mode("append").saveAsTable("naval.emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.emp

# COMMAND ----------


