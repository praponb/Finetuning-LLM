from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.shipping_fee") <= 14)
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.method"),
        F.col("o.discount"),
        F.trim(F.col("o.channel")).alias("clean_channel"),
    )
)
