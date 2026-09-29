from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.region") == "South")
    .select(
        F.col("s.region"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("o.channel"),
    )
)
