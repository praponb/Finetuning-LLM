from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.order_id").isin([10, 16, 24, 31]) | F.col("o.store_id").isNotNull())
    .select(
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
    )
)
