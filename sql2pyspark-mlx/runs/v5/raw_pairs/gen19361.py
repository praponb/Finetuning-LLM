from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") >= "2025-01-01") & F.col("order_id").isin([4, 25, 28, 32]) & (F.col("cost") >= 33))
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("stddev_cost") < 37)
    .orderBy(F.col("carrier").desc(), F.col("avg_cost").desc())
)
