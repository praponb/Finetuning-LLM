from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.status") != "FAILED")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("p.status"),
        F.col("p.paid_at"),
    )
    .orderBy(F.col("customer_id").desc())
)
