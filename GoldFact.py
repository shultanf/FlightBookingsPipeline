# Databricks notebook source
key_cols_list = eval("['booking_id', 'passenger_id', 'airport_id','flight_id']")
# key_cols_list = eval("['DimPassengersKey','DimFlightsKey','DimAirportsKey','booking_id']")
cdc_col = "ingested_at_silver"
#cdc_col_gold = ""
backdated_refresh = ""

source_tbl = "silver_bookings"
source_schema = "silver"
source_tbl_path = f"workspace.{source_schema}.{source_tbl}"

target_tbl = "factBookings"
target_schema = "gold"
surrogate_key = "factBookingsKey"

# COMMAND ----------

dimensions = [
    {
        "table": f"workspace.{target_schema}.dimpassengers",
        "alias": "dimpassengers",
        "surrogate_key": "dimPassengerKey",
        "join_keys": [("passenger_id","passenger_id")] # (fact_col, dim_col)
    },
    {
        "table": f"workspace.{target_schema}.dimflights",
        "alias": "dimflights",
        "surrogate_key": "dimFlightKey",
        "join_keys": [("flight_id","flight_id")] # (fact_col, dim_col)
    },
    {
        "table": f"workspace.{target_schema}.dimairports",
        "alias": "dimairports",
        "surrogate_key": "dimAirportKey",
        "join_keys": [("airport_id","airport_id")] # (fact_col, dim_col)
    }
]


fact_column = ["booking_id","amount","booking_date","ingested_at_silver","airport_id","flight_id","passenger_id"] # Cols to keep from the fact table (besides surrogate _key)

# COMMAND ----------

isTblExist = spark.catalog.tableExists(f"workspace.{target_schema}.{target_tbl}")
if len(backdated_refresh) == 0:
    if isTblExist == True:
        last_load = spark.sql(f"SELECT max({cdc_col}) FROM workspace.{target_schema}.{target_tbl}").collect()[0][0]
    else:
        last_load = "1900-01-01 00:00:00"
else:
    last_load = backdated_refresh

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Fact Query

# COMMAND ----------

def generate_fact_query_incremental(fact_tbl, dimensions, fact_columns, cdc_column, processing_date):
    fact_alias = "f"
    
    # Base columns to select
    select_cols = [f"{fact_alias}.{c}" for c in fact_columns]

    # Build joins dynamically
    join_clauses = []
    for dim in dimensions:
        tbl_full = dim["table"]
        tbl_name = tbl_full.split(".")[-1]
        tbl_alias = dim["alias"]
        
        col_surrogate_key = f"{tbl_alias}.{dim["surrogate_key"]}"
        select_cols.append(col_surrogate_key)
        
        # Build ON clause
        on_conditions = [f"{fact_alias}.{fk} = {tbl_alias}.{pk}" for fk, pk in dim["join_keys"]]

        join_clause = f"LEFT JOIN {tbl_full} {tbl_alias} ON " + "AND".join(on_conditions)
        join_clauses.append(join_clause)
    
    where_clause = f"{fact_alias}.{cdc_col} > '{last_load}'"
    join_clauses = "\n".join(join_clauses)
    select_clauses = ", ".join(select_cols)
    
    final_query = f"""
    SELECT 
        {select_clauses}
    FROM 
        {fact_tbl} {fact_alias}
        {join_clauses}
    WHERE 
        {where_clause}
    """

    return final_query

# COMMAND ----------

query = generate_fact_query_incremental(fact_tbl=source_tbl_path, 
                                        dimensions=dimensions, 
                                        fact_columns=fact_column, 
                                        cdc_column=cdc_col, 
                                        processing_date=last_load)


# COMMAND ----------

print(query)

# COMMAND ----------

df_fact = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT

# COMMAND ----------

# Fact Key Columns Merge Condition
fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in key_cols_list])
fact_key_cols_str

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"workspace.{target_schema}.{target_tbl}"):
    dlt_obj = DeltaTable.forName(spark, f"workspace.{target_schema}.{target_tbl}")
    dlt_obj.alias("trg") \
            .merge(df_fact.alias("src"),
                   'src.booking_id = trg.booking_id') \
            .whenMatchedUpdateAll(condition = f"src.{cdc_col} > trg.{cdc_col}")\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    df_fact.write.format("delta").mode("append").saveAsTable(f"workspace.{target_schema}.{target_tbl}")