from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.hour(F.col("p.paid_at")).between(6, 11))
    .groupBy(
        F.when(F.col("o.discount") >= 0.34, "high").when(F.col("o.discount") >= 0.07, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.min(F.col("p.paid_at")).alias("min_paid_at"),
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
    )
)
