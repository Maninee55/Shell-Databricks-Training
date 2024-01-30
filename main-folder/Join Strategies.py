# Databricks notebook source
Strategies in Spark
1. Sort Merge Join
2. Broadcast Hash Join
3. Shuffle Hash Join
4. broadcast nested loop join

# COMMAND ----------

customer_data = [(1001, 'Alice', 'Johnson', '2024-01-15'),
(1002, 'Bob', 'Smith', '2024-01-18'),
(1003, 'Carol', 'Davis', '2024-01-22'),
(1004, 'David', 'Miller', '2024-01-25'),
(1005, 'Emily', 'Martinez', '2024-01-28'),
(1006, 'Frank', 'Taylor', '2024-01-30'),
(1007, 'Grace', 'Anderson', '2024-02-02'),
(1008, 'Harry', 'White', '2024-02-05'),
(1009, 'Iris', 'Brown', '2024-02-08'),
(1010, 'Jack', 'Wilson', '2024-02-12')]

customer_schema = ['Customer_id','First_name','Last_name','Order_date']

customer_df = spark.createDataFrame(data=customer_data,schema=customer_schema)

# COMMAND ----------

customer_df.display()

# COMMAND ----------

sales_data = [
    (1, 1001, 'ProductA', 2, 50.0, '2024-01-15'),
    (2, 1002, 'ProductB', 1, 75.0, '2024-01-18'),
    (3, 1003, 'ProductC', 3, 30.0, '2024-01-22'),
    (4, 1004, 'ProductA', 1, 50.0, '2024-01-25'),
    (5, 1005, 'ProductB', 2, 75.0, '2024-01-28'),
    (6, 1006, 'ProductC', 1, 30.0, '2024-01-30'),
    (7, 1007, 'ProductA', 2, 50.0, '2024-02-02'),
    (8, 1008, 'ProductB', 1, 75.0, '2024-02-05'),
    (9, 1009, 'ProductC', 3, 30.0, '2024-02-08'),
    (10, 1010, 'ProductA', 1, 50.0, '2024-02-12'),
    (11, 1002, 'ProductB', 1, 75.0, '2024-01-18'), 
    (12, 1006, 'ProductC', 1, 30.0, '2024-01-30')]

sales_schema = ['OrderID','CustomerID','Product','Quantity','Price','OrderDate']
sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)

# COMMAND ----------

sales_df.display()

# COMMAND ----------

df=customer_df.join(sales_df,customer_df["Customer_id"]==sales_df["CustomerID"])

# COMMAND ----------

df.display()

# COMMAND ----------

df.explain()

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",5)

# COMMAND ----------

df.display()

# COMMAND ----------

df.explain()

# COMMAND ----------

Tusu7056
Rano5591

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

df=customer_df.join(broadcast(sales_df),customer_df["Customer_id"]==sales_df["CustomerID"])

# COMMAND ----------

df.explain()

# COMMAND ----------

df.display()

# COMMAND ----------

hysical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- SortMergeJoin [Customer_id#1000L], [CustomerID#1022L], Inner
   :- Sort [Customer_id#1000L ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(Customer_id#1000L, 5), ENSURE_REQUIREMENTS, [plan_id=2160]
   :     +- Filter isnotnull(Customer_id#1000L)
   :        +- Scan ExistingRDD[Customer_id#1000L,First_name#1001,Last_name#1002,Order_date#1003]
   +- Sort [CustomerID#1022L ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(CustomerID#1022L, 5), ENSURE_REQUIREMENTS, [plan_id=2161]
         +- Filter isnotnull(CustomerID#1022L)
            +- Scan ExistingRDD[OrderID#1021L,CustomerID#1022L,Product#1023,Quantity#1024L,Price#1025,OrderDate#1026]
