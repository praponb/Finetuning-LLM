from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("carrier") != "Kerry")
    .groupBy("order_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.min("shipped_date").alias("min_shipped_date"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .filter(F.col("O.avg_cost") > 44)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("O.avg_cost"),
        F.col("O.min_shipped_date"),
    )
)
