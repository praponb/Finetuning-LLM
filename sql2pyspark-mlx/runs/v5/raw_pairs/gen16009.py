from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("order_id").isin([14, 16]))
    .groupBy("order_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("O.avg_quantity"),
    )
    .orderBy("store_id", F.col("avg_quantity").desc())
)
