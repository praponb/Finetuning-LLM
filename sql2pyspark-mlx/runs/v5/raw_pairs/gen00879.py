from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .filter(F.col("p.order_id") == 10)
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("o.order_date"),
    )
    .orderBy("paid_at", F.col("id").desc())
)
