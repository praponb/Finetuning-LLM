from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("s.city"),
    )
    .orderBy(F.col("id").desc())
)
