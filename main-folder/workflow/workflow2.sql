-- Databricks notebook source
create table if not exists dev.work.energy_clean as select country, year,coal_production,electricity_demand,electricity_generation from dev.work.energy

-- COMMAND ----------

create table if not exists dev.work.energybyyear as select year,sum(electricity_generation) total_generation from dev.work.energy_clean group by year
