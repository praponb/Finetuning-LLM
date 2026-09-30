from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.min(F.col("p.paid_at")).alias("min_paid_at"),
    )
)
