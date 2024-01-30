-- Databricks notebook source
create catalog if not exists uat;
use catalog uat

-- COMMAND ----------

create schema example;
use schema example

-- COMMAND ----------


CREATE OR REPLACE TABLE heartrate_device (device_id INT, mrn STRING, name STRING, time TIMESTAMP, heartrate DOUBLE);

INSERT INTO heartrate_device VALUES
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:01:58.000+0000", 54.0122153343),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:02:55.000+0000", 92.5136468131),
  (37, "65300842", "Samuel Hughes", "2020-02-01T00:08:58.000+0000", 52.1354807863),
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:16:51.000+0000", 54.6477014191),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:18:08.000+0000", 95.033344842);
  
SELECT * FROM heartrate_device

-- COMMAND ----------

create view agg_heartrate as 
(select mrn,name, mean(heartrate) avg_heartrate
from heartrate_device 
group by all)

-- COMMAND ----------

GRANT USAGE on CATALOG dev to `test`;

-- COMMAND ----------

GRANT USAGE on schema dev.naval to `test`;

-- COMMAND ----------

GRANT CREATE on schema dev.naval to `test`;

-- COMMAND ----------

select * from uat.`shell10-ngp`.export_shell_10

-- COMMAND ----------


