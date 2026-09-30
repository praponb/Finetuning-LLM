from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
    )
    .orderBy(F.col("avg_amount").desc(), "method")
    .limit(1)
)
