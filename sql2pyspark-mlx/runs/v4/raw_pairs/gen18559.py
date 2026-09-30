from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2025-06-01") & (F.col("amount") < 540))
    .groupBy("status", "method")
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
    )
    .orderBy(F.col("unique_statuss").desc(), "method", "status")
    .limit(50)
)
