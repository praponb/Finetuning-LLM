from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "right")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
    )
    .orderBy(F.col("unique_orders").desc(), "channel")
    .limit(50)
)
