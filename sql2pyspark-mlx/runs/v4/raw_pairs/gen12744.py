from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
    )
    .filter(F.col("in_transit_count") < 28)
    .select("status", "in_transit_count", "pct_returned")
)
