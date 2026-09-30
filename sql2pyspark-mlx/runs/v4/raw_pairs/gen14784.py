from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.max(F.col("p.paid_at")).alias("latest_paid_at"),
        F.min(F.col("p.paid_at")).alias("min_paid_at"),
    )
    .orderBy("status")
)
