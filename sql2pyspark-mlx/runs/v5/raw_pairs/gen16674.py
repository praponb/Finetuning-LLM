from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("s.order_id"),
        F.col("s.delivered_date"),
        F.col("s.status"),
        F.col("o.shipping_fee"),
        F.col("o.status").alias("order_status"),
        F.col("o.discount"),
        F.initcap(F.trim(F.col("o.channel"))).alias("channel_title"),
    )
)
