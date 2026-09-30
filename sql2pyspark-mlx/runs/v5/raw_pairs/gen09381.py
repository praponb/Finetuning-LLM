from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("s.region"),
    )
)
