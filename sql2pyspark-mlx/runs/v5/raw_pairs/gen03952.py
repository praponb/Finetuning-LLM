from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("kerry_count") > 2)
)
