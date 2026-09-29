from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter((F.to_date("paid_at") >= "2024-02-01") | F.col("status").isin(["REFUNDED", "FAILED"]))
)

payments_stats = (
    filtered_payments
    .groupBy("method", "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
)

result = (
    payments_stats
    .filter(F.col("pct_refunded") < 61)
    .select("method", "status")
    .orderBy(F.col("method").desc())
)
