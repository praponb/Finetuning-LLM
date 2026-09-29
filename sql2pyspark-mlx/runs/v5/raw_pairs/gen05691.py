from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
    )
    .orderBy(F.col("channel").desc(), "id")
    .limit(10)
)
