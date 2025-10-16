# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Incremental Data Ingestion**

# COMMAND ----------

dbutils.widgets.text("src","")

# COMMAND ----------

src_value = dbutils.widgets.get("src")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloader Stream

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","csv")\
            .option("cloudFiles.schemaLocation",f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
            .option("cloudFiles.schemaEvolutionMode","rescue")\
            .load(f"/Volumes/workspace/raw/rawvolume/rawdata/{src_value}")

# COMMAND ----------

df.writeStream.format("delta")\
        .outputMode("append")\
        .trigger(once=True)\
        .option("checkpointLocation",f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
        .option("path",f"/Volumes/workspace/bronze/bronzevolume/{src_value}/data")\
        .start()