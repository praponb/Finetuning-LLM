from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region") == "West")
    .select(
        F.col("o.store_id"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("s.sqft"),
        F.col("s.region"),
    )
)
