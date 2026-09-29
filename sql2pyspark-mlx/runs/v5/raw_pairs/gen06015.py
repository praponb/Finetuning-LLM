from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") < "2024-07-01") & F.col("is_flagged"))
    .groupBy("is_flagged", "merchant")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.count("amount").alias("amount_count"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("amount_count").desc())
)
