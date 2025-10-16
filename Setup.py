# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw_test.rawvolume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.rawvolume

# COMMAND ----------

data = ['bookings','airports','passengers','flights']
for i in data:
  dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/"+i)

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/bronze/bronzevolume/bookings/data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME workspace.gold.goldvolume

# COMMAND ----------

