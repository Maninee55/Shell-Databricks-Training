-- Databricks notebook source
-- MAGIC %python
-- MAGIC print("run python")

-- COMMAND ----------

select "RUN SQL"

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC println("RUn Scala")

-- COMMAND ----------

create database demo

-- COMMAND ----------

drop database demo cascade

-- COMMAND ----------

use demo

-- COMMAND ----------

create table emp (id int, name varchar(30));
insert into emp values(1,'a')

-- COMMAND ----------

select * from emp

-- COMMAND ----------


