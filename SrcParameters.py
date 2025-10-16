# Databricks notebook source
src_array = [
    {"src":"bookings"},
    {"src":"flights"},
    {"src":"airports"},
    {"src":"passengers"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key="output_key", value=src_array)

# COMMAND ----------

