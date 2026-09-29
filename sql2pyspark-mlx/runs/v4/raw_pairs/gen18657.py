from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]) & (F.col("paid_at") < "2025-09-27"))
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy("paid_month")
)
