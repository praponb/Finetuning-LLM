from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .select(F.col("o.shipping_fee"), F.col("p.paid_at"))
    .orderBy(F.col("paid_at").desc())
)
