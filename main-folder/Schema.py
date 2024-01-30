# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

df.show(10)

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Schema?
# MAGIC - if you donot have header,
# MAGIC - if you large dataset 
# MAGIC ### creating our own schema
# MAGIC - string (simple data)
# MAGIC - list
# MAGIC - pyspark style(nested data)

# COMMAND ----------

data=[(1,'a',30),(2,'b',35)]
df=spark.createDataFrame(data)

# COMMAND ----------

df.display()

# COMMAND ----------

data=[(1,'a',30),(2,'b',35)]
schema="id int, name string, age int"
dfstr=spark.createDataFrame(data,schema)
dfstr.display()
dfstr.printSchema()

# COMMAND ----------

data=[{"id":1,"name":"a","age":30},{"id":2,"name":"b","age":25}]
schema2="id int, name string, age int"
df2=spark.createDataFrame(data,schema2)
df2.display()
df2.printSchema()

# COMMAND ----------

data=[{"id":1,"name":"a","age":30,"mobile":1},{"id":2,"name":"b","age":25,"mobile":91}]
schema2="id int, name string, age int"
df2=spark.createDataFrame(data,schema2)
df2.display()
df2.printSchema()

# COMMAND ----------

data=[{"id":1,"name":"a","age":30,"mobile":[1, 91]},{"id":2,"name":"b","age":25,"mobile":[91,44]}]
schema2="id int, name string, age int, mobile array"
df2=spark.createDataFrame(data,schema2)
df2.display()
df2.printSchema()

# COMMAND ----------

data=[{"id":1,"name":"a","age":30,"mobile":[1, 91]},{"id":2,"name":"b","age":25,"mobile":[91,44]}]
schema2="id int, name string, age int, mobile string"
df2=spark.createDataFrame(data,schema2)
df2.display()
df2.printSchema()

# COMMAND ----------

data=[{"id":1,"name":"a","age":30,"mobile":{"home":1, "office":91}},{"id":2,"name":"b","age":25,"mobile":{"home":91, "office":44}}]
schema2="id int, name string, age int, mobile string"
df2=spark.createDataFrame(data,schema2)
df2.display()
df2.printSchema()

# COMMAND ----------

PySpark Data type
1. Struct
2. Map
3. Array

# COMMAND ----------

data=[{"id":1,"name":"a","age":30,"mobile":1},{"id":2,"name":"b","age":25,"mobile":91}]
df2=spark.createDataFrame(data,schema2)
df2.display()
df2.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

users_schema= StructType([StructField("id",IntegerType()),
                          StructField("name",StringType()),
                          StructField("age",IntegerType())
                          ])

# COMMAND ----------

data=[{"id":1,"name":"a","age":30,"mobile":1},{"id":2,"name":"b","age":25,"mobile":91}]
df2=spark.createDataFrame(data,users_schema)
df2.display()
df2.printSchema()

# COMMAND ----------

data=[{"id":1,"name":"a","age":30,"mobile":[1, 91]},{"id":2,"name":"b","age":25,"mobile":[91,44]}]
schema2="id int, name string, age int, mobile string"
df2=spark.createDataFrame(data,schema2)
df2.display()
df2.printSchema()

# COMMAND ----------

data=[{"id":1,"name":"a","age":30,"mobile":[1, 91,81]},{"id":2,"name":"b","age":25,"mobile":[91]}]
df2=spark.createDataFrame(data,users_schema2)
df2.display()
df2.printSchema()

# COMMAND ----------

(df2.withColumn("home",df2.mobile[0])
.withColumn("office",df2.mobile[1])
.drop("mobile")
.display())

# COMMAND ----------

(df2.withColumn("home",col("mobile")[0])
.withColumn("office",col("mobile")[1])
.drop("mobile")
.display())

# COMMAND ----------

df2.withColumn("mobile",explode("mobile")).display()

# COMMAND ----------

users_schema2= StructType([StructField("id",IntegerType()),
                          StructField("name",StringType()),
                          StructField("age",IntegerType()),
                          StructField("mobile",ArrayType(IntegerType()))
                          ])

# COMMAND ----------

users_schema3= StructType([StructField("id",IntegerType()),
                          StructField("name",StringType()),
                          StructField("age",IntegerType()),
                          StructField("mobile",MapType(StringType(),IntegerType()))
                          ])

# COMMAND ----------

data=[
    {"id":1,"name":"a","age":30,"mobile":{"home":1, "office":91}},
      {"id":2,"name":"b","age":25,"mobile":{"home":91, "office":44}}
      ]
df3=spark.createDataFrame(data,users_schema3)
df3.display()
df3.printSchema()

# COMMAND ----------

df3.withColumn("office",col("mobile.office")).withColumn("home",col("mobile.home")).drop("mobile").display()

# COMMAND ----------

data=[
    {"id":1,"name":"a","age":30,"mobile":{"home":1, "office":91}},
      {"id":2,"name":"b","age":25,"mobile":{"home":91, "office":44}}
      ]
df2=spark.createDataFrame(data)
df2.display()
df2.printSchema()

# COMMAND ----------


