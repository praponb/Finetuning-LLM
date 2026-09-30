from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.amount") >= 820) & (F.col("o.discount") >= 0.14))
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.status"),
        F.col("p.order_id"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("o.discount"),
    )
    .orderBy("status", "id")
    .limit(3)
)
