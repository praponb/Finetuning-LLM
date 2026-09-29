from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", "carrier")
    .agg(
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)
