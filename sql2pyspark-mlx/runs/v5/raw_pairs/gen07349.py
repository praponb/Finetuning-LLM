from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
    .groupBy(
        F.when(F.col("amount") >= 1570, "high").when(F.col("amount") >= 590, "mid").otherwise("low").alias("amount_bucket"),
        "status",
        "method",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
    )
    .orderBy("method", "status", "amount_bucket")
    .limit(3)
)
