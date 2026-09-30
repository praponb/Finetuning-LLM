from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left")
    .filter(F.col("orders.status") == "CANCELLED")
    .groupBy(F.col("orders.id"))
    .agg(
        F.count(F.col("order_items.id")).alias("order_item_count"),
        F.count(F.col("order_items.quantity")).alias("quantity_count"),
    )
    .filter(F.col("order_item_count") > 2)
    .orderBy(F.col("id").desc())
    .limit(3)
)
