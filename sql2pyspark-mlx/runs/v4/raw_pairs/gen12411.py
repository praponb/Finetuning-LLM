from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .select(
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("p.order_id"),
        F.col("p.status"),
        F.col("p.paid_at"),
    )
)
