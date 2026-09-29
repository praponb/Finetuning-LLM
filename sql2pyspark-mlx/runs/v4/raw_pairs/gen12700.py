from pyspark.sql import functions as F

base = df_transactions.filter(F.col("is_flagged"))

summary = (
    base
    .groupBy("txn_type", F.month("txn_time").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
        F.min("txn_time").alias("min_txn_time"),
    )
)

result = summary.filter(F.col("starbucks_amount") > 36)
