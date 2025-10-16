import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

###################
## Bookings Data ##
###################

# Load data from Bronze
@dlt.view(
    name = "stage_bookings"
)
def stage_bookings():
    return spark.readStream.format("delta")\
                .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")

# Transform
@dlt.view(
    name = "transform_bookings"
)
def transform_bookings():
    return spark.readStream.table("stage_bookings")\
            .withColumn("amount", col("amount").cast(DoubleType()))\
            .withColumn("booking_date", to_date(col("booking_date")))\
            .drop("_rescued_data")

# Silver
rules = {
    "rule1":"booking_id IS NOT NULL",
    "rule2":"passenger_id IS NOT NULL"
}

@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    return spark.readStream.table("transform_bookings")\
                .withColumn("ingested_at_silver", current_timestamp())
    

##################
## Flights Data ##
##################

# Declare dlt view
@dlt.view(
    name = "trans_flights"
)
# Declare expectation
@dlt.expect_or_fail("valid_flight_id","flight_id IS NOT NULL")

# Load data from Bronze
def trans_flights():
    return spark.readStream.format("delta")\
                .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")\
                .withColumn("flight_date", to_date(col("flight_date")))\
                .withColumn("ingested_at_silver", current_timestamp())\
                .drop("_rescued_data")
    

## Create dlt target table
dlt.create_streaming_table("silver_flights")

## Create auto CDC flow
dlt.create_auto_cdc_flow(
  target = "silver_flights",
  source = "trans_flights",
  keys = ["flight_id"],
  sequence_by = col("ingested_at_silver"),
  stored_as_scd_type = 1
)


#####################
## Passengers Data ##
#####################

@dlt.view(
    name = "trans_passengers"
)
@dlt.expect_or_fail("valid_passenger_id","passenger_id IS NOT NULL")
def trans_passengers():
  return spark.readStream.format("delta")\
                .load("/Volumes/workspace/bronze/bronzevolume/passengers/data/")\
                .withColumn("ingested_at_silver", current_timestamp())\
                .drop("_rescued_data")
  

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
  target = "silver_passengers",
  source = "trans_passengers",
  keys = ["passenger_id"],
  sequence_by = col("ingested_at_silver"),
  stored_as_scd_type = 1
)


###################
## Airports Data ##
###################

@dlt.view(
    name = "trans_airports"
)
@dlt.expect_or_fail("valid_airport_id","airport_id IS NOT NULL")
def trans_airports():
  return spark.readStream.format("delta")\
                .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")\
                .withColumn("ingested_at_silver", current_timestamp())\
                .drop("_rescued_data")
  

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
  target = "silver_airports",
  source = "trans_airports",
  keys = ["airport_id"],
  sequence_by = col("ingested_at_silver"),
  stored_as_scd_type = 1
)

##########################
## Silver Business VIEW ##
##########################

@dlt.view(
    name = "silver_business"
)
def silver_busines():
    
    # Rename "ingested_to_silver_ts" column
    bookings_df = dlt.read("silver_bookings").withColumnRenamed("ingested_at_silver","bookings_ingested_at_silver")
    passengers_df = dlt.read("silver_passengers").withColumnRenamed("ingested_at_silver","passengers_ingested_at_silver")
    flights_df = dlt.read("silver_flights").withColumnRenamed("ingested_at_silver","flights_ingested_at_silver")
    airports_df = dlt.read("silver_airports").withColumnRenamed("ingested_at_silver","airports_ingested_at_silver")

    # Join the tables
    return bookings_df\
            .join(flights_df,"flight_id")\
            .join(passengers_df,"passenger_id")\
            .join(airports_df,"airport_id")


#######################################
## Silver Business Materialized VIEW ##
#######################################

@dlt.table(
    name = "silver_business_materialized"
)
def silver_busines_materialized():
    return dlt.read("silver_business")
    












