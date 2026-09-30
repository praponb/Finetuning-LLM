from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("merchant") == "Starbucks") & (F.col("txn_time") < "2025-05-25")) | F.col("is_flagged"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
    )
    .orderBy("pct_shell")
)
