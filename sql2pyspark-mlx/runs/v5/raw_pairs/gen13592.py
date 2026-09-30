from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.discount").isNotNull())
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
    )
    .orderBy(F.col("order_item_count").desc(), "channel")
    .limit(3)
)
