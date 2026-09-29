from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
    )
    .filter(F.col("order_item_count") >= 1)
    .orderBy(F.col("total_quantity").desc(), F.col("order_item_count").desc(), "status")
    .limit(3)
)
