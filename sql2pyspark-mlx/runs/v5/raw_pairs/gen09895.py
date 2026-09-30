from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "bank_transfer"]) & F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
    .withColumn("paid_at_month", F.month("paid_at"))
)

payments_stats = (
    src
    .groupBy("status")
    .agg(
        F.count("*").alias("row_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
    )
)

result = (
    payments_stats
    .orderBy(F.col("pct_success").desc(), "status")
    .limit(5)
)
