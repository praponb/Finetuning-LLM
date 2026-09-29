from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.hour(F.col("p.paid_at")).between(8, 12))
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.method"),
        F.col("p.paid_at"),
        F.col("o.discount"),
        F.round((F.col("o.discount") * 1.1), 2).alias("discount_scaled"),
    )
    .orderBy(F.col("id").desc(), F.col("discount_scaled").desc())
    .limit(5)
)
