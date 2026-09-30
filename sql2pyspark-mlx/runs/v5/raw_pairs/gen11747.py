from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("o.channel"),
    )
)
