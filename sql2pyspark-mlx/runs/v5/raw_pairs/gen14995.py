from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "right")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("s.store_name"),
    )
    .orderBy("status")
)
