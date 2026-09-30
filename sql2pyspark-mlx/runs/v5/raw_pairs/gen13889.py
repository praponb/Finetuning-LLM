from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.hour(F.col("p.paid_at")).between(11, 17))
    .select(
        F.col("p.method"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.substring(F.col("o.channel"), 1, 3).alias("channel_prefix"),
    )
    .orderBy(F.col("channel_prefix").desc())
)
