from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1300, 3000))
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .filter(F.col("pct_shell") >= 67)
)
