from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy(
        "merchant",
        F.when(F.col("amount") >= 3700, "high").when(F.col("amount") >= 1300, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
    )
    .filter(F.col("refund_count") > 4)
    .orderBy("merchant", F.col("unique_accounts").desc())
)
