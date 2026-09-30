from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter((F.col("o.amount") <= 520) | (F.col("s.cost") <= 59))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.upper(F.col("o.channel")).alias("channel_upper"),
    )
)
