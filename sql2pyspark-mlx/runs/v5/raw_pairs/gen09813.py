from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.col("s.opened_date"),
        F.coalesce(F.col("o.shipping_fee"), F.lit(0)).alias("shipping_fee_filled"),
    )
    .orderBy("shipping_fee")
)
