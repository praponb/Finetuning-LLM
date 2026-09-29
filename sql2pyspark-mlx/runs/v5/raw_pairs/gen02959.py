from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.min("amount").alias("min_amount"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
    )
    .orderBy(F.col("pct_debit").desc())
)
