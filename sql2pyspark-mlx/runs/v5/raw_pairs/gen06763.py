from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter((F.col("oi.quantity") >= 5) | F.col("oi.order_id").isin([34, 36]))
    .select(
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("oi.order_id"),
    )
    .orderBy("customer_id", "amount")
)
