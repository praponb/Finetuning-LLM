from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("o.store_id").isin([9, 12, 19, 33]))
    .select(
        F.col("s.store_name"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
    )
)
