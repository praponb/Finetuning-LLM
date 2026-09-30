from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & ((F.col("paid_at") >= "2025-08-01") | (F.col("status") == "REFUNDED")))
    .groupBy("status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)
