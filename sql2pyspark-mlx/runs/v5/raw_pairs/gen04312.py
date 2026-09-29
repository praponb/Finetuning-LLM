from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy(
        "txn_type",
        F.when(F.col("amount") >= 3300, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
    )
    .filter(F.col("pct_fee") < 38)
    .orderBy("amount_bucket")
)
