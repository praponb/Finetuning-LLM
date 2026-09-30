from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("oi.order_id").isin([31, 34, 40]) & (F.col("s.sqft") < 12392))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("store_name").desc())
)
