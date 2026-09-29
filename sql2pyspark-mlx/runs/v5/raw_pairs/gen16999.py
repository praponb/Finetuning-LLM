from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .select(
        F.col("oi.product_id"),
        F.col("o.channel"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
)
