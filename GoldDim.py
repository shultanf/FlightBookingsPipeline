# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

key_cols_list = eval("['flight_id']")
cdc_col_silver = "ingested_at_silver"
cdc_col_gold = "ingested_at_gold"
backdated_refresh = ""
source_tbl = "silver_flights"
source_schema = "silver"
target_tbl = "dimFlights"
target_schema = "gold"
surrogate_key = "dimFlightKey"

# COMMAND ----------

# key_cols_list = eval("['airport_id']")
# cdc_col_silver = "ingested_at_silver"
# backdated_refresh = ""
# source_tbl = "silver_airports"
# source_schema = "silver"
# target_tbl = "dimAirports"
# target_schema = "gold"
# surrogate_key = "dimAirportKey"

# COMMAND ----------

# key_cols_list = eval("['passenger_id']")
# cdc_col_silver = "ingested_at_silver"
# backdated_refresh = ""
# source_tbl = "silver_passengers"
# source_schema = "silver"
# target_tbl = "dimPassengers"
# target_schema = "gold"
# surrogate_key = "dimPassengerKey"

# COMMAND ----------

###########################
## Get [last_load] value ##
###########################

# Check if table exist in gold/target schema
isTblExist = spark.catalog.tableExists(f"workspace.{target_schema}.{target_tbl}")
if len(backdated_refresh) == 0:
    if isTblExist == True:
        last_load = spark.sql(f"SELECT max({cdc_col_silver}) FROM workspace.{target_schema}.{target_tbl}").collect()[0][0]
    else:
        last_load = "1900-01-01 00:00:00"
else:
    last_load = backdated_refresh

# COMMAND ----------

df_source = spark.sql(f"""
                      SELECT * 
                      FROM workspace.{source_schema}.{source_tbl} 
                      WHERE {cdc_col_silver} > '{last_load}'
                      """)

# COMMAND ----------

# MAGIC %md
# MAGIC > ### Incremental Ingestion *from Silver to Gold* ###
# MAGIC - Join Silver table with Gold table (if initial load, will be joined with empty table) 
# MAGIC - then separate the old record with the new record.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join **_Silver_** table and **_Gold_** table #####

# COMMAND ----------


if isTblExist == True:
    sql_key_cols_string = ', '.join(key_cols_list)
    df_target = spark.sql(f"""
                          SELECT {sql_key_cols_string}, 
                          {surrogate_key}, 
                          created_at, 
                          updated_at 
                          FROM workspace.{target_schema}.{target_tbl}
                          """) 
else:
    key_cols_list_init = [f"'' AS {i}" for i in key_cols_list]
    key_cols_list_init = ','.join(key_cols_list_init)
    df_target = spark.sql(f"""
                          SELECT {key_cols_list_init},
                          CAST('0' AS int) AS {surrogate_key}, 
                          CAST('1900-01-01 00:00:00' AS timestamp) AS created_at, 
                          CAST('1900-01-01 00:00:00' AS timestamp) AS updated_at 
                          WHERE 1=0
                          """)

# COMMAND ----------

join_condition = ' AND '.join([f"source.{i} = target.{i}" for i in key_cols_list])
join_condition

# COMMAND ----------

df_source.createOrReplaceTempView("source")
df_target.createOrReplaceTempView("target")

# COMMAND ----------

# df_source.display()
# df_target.display()

# COMMAND ----------

df_join = spark.sql(f"""
                    SELECT 
                    source.*,
                    target.{surrogate_key},
                    target.created_at,
                    target.updated_at
                    FROM source
                    LEFT JOIN target
                    ON
                    {join_condition}
                    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Separate the old (Silver) with new (Gold) table.

# COMMAND ----------

from pyspark.sql.functions import col
df_old = df_join.filter(col(surrogate_key).isNotNull())

df_new = df_join.filter(col(surrogate_key).isNull())

# df_old.display()
# df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare DF_OLD and DF_NEW to be union into Gold Layer 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Preparing df_old #####

# COMMAND ----------

df_old_enriched = df_old.withColumn("updated_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Prearing df_new #####

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_tbl}") == True:
    max_surrogate_key = spark.sql(f"SELECT max({surrogate_key}) FROM workspace.{target_schema}.{target_tbl}").collect()[0][0]
    incr_surrogate_key = lit(max_surrogate_key)+monotonically_increasing_id()

    df_new_enriched = df_new.withColumn(f"{surrogate_key}", incr_surrogate_key)\
                            .withColumn("created_at", current_timestamp())\
                            .withColumn("updated_at", current_timestamp())
else:
    max_surrogate_key = 0
    incr_surrogate_key = lit(max_surrogate_key)+lit(1)+monotonically_increasing_id()
    
    df_new_enriched = df_new.withColumn(f"{surrogate_key}", incr_surrogate_key)\
                            .withColumn("created_at", current_timestamp())\
                            .withColumn("updated_at", current_timestamp())

# COMMAND ----------

df_union = df_old_enriched.unionByName(df_new_enriched)
df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_tbl}") == True:
    delta_obj = DeltaTable.forName(spark, f"workspace.{target_schema}.{target_tbl}")
    delta_obj.alias("target")\
            .merge(df_union.alias("source"), f"target.{surrogate_key} = source.{surrogate_key}")\
            .whenMatchedUpdateAll(condition = f"source.{cdc_col_silver} > target.{cdc_col_silver}")\
            .whenNotMatchedInsertAll()\
            .execute()

else:
    df_union.write.format("delta")\
                    .mode("append")\
                    .saveAsTable(f"workspace.{target_schema}.{target_tbl}")

# COMMAND ----------

spark.sql(f"SELECT * FROM workspace.{target_schema}.{target_tbl}").display()
