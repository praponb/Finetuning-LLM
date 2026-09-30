from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([9, 14, 18, 21]) & (F.to_date("paid_at") < "2024-02-16") & (F.col("status") == "REFUNDED"))
    .groupBy("status")
    .agg(
        F.count("*").alias("payment_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("avg_amount").desc(), "status")
    .limit(10)
)
