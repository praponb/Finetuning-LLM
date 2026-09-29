from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([11, 22, 24]) & F.col("order_id").isin([11, 30]) & F.col("order_id").isNotNull())
    .groupBy("method")
    .agg(
        F.count("*").alias("payment_count"),
        F.max("paid_at").alias("last_paid_at"),
    )
    .filter(F.col("payment_count") < 1)
    .orderBy("method")
)
