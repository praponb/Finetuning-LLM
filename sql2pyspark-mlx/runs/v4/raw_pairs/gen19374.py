from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.amount") >= 1530)
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
    )
    .orderBy("id", "channel")
    .limit(50)
)
