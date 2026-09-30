from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.status") == "RETURNED")
    .groupBy(F.col("orders.channel"))
    .agg(
        F.count(F.col("order_items.id")).alias("order_item_count"),
        F.avg(F.col("order_items.unit_price")).alias("avg_unit_price"),
    )
    .orderBy("channel")
)
