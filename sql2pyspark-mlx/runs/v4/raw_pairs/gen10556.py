from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 18)
    .groupBy(
        "status",
        F.when(F.col("amount") >= 1360, "high").when(F.col("amount") >= 470, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
    )
    .filter(F.col("pct_failed") > 25)
    .orderBy(F.col("pct_failed").desc())
)
