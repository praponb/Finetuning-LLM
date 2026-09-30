from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") >= 2023)
    .groupBy(F.year("delivered_date").alias("delivered_year"))
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("in_transit_cost") < 32)
    .select("delivered_year", "in_transit_cost", "stddev_cost")
)
