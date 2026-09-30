from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "RETURNED")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
        F.count("*").alias("num_shipments"),
    )
    .orderBy(F.col("in_transit_count").desc())
)
