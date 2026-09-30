from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .groupBy("order_id")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.stddev_cost") >= 33)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("t.cost_range"),
        F.col("t.stddev_cost"),
    )
    .orderBy("channel")
)
