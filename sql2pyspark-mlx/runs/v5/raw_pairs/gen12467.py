from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("o.store_id"),
        F.col("o.status"),
        F.col("o.customer_id"),
    )
    .orderBy("status", F.col("store_id").desc())
)
