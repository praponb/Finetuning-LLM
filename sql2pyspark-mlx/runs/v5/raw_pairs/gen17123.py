from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.paid_at") >= "2023-06-20")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.col("o.channel"),
        F.col("p.paid_at"),
        F.col("p.order_id"),
    )
)
