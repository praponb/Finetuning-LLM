from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("o.amount") >= 920) & F.col("oi.product_id").isin([11, 26, 36, 39]))
    .select(
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("o.channel"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
    .orderBy("channel")
)
