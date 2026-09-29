from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", F.month("txn_time").alias("txn_month"), "account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
    )
    .filter(F.col("pct_fee") >= 32)
    .orderBy(F.col("account_id").desc(), F.col("pct_fee").desc())
)
