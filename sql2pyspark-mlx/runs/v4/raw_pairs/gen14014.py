from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("is_flagged"))

agg = (
    recent_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
        F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0)).alias("grab_count"),
    )
)

result = (
    agg
    .filter(F.col("pct_refund") > 58)
    .select("txn_month", "pct_refund")
)
