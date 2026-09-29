from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("o.channel"),
        F.least(F.col("o.amount"), F.col("o.shipping_fee")).alias("least_amount_shipping_fee"),
    )
    .orderBy(F.col("channel").desc())
)
