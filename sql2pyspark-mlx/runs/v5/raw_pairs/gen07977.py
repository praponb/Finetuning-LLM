from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("oi.order_id"),
        F.col("o.store_id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
    )
)
