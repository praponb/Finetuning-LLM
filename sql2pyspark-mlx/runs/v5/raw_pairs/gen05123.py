from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .select(
        F.col("p.order_id"),
        F.col("p.amount"),
        F.col("p.paid_at"),
        F.col("o.channel"),
        F.col("o.store_id"),
    )
)
