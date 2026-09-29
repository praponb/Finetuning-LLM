from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.channel").isin(["web", "mobile"]))
    .select(
        F.col("orders.store_id"),
        F.col("orders.amount"),
        F.col("orders.discount"),
        F.col("order_items.order_id"),
    )
    .orderBy("order_id")
)
