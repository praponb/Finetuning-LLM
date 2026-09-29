from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([3, 18, 26, 35]))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
    )
    .orderBy(F.col("pct_fee").desc())
)
