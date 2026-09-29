from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.customer_id") == 35) & F.col("o.store_id").isNotNull())
    .select(
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("o.discount"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.concat(F.col("o.channel"), F.lit(", "), F.col("o.status")).alias("channel_status"),
    )
)
