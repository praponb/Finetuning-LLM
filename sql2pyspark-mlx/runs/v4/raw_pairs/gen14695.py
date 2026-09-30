from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
        F.sum(F.col("oi.unit_price")).alias("total_unit_price"),
    )
    .orderBy("order_item_count")
)
