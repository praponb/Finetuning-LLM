from pyspark.sql import functions as F

matched_keys = (
    df_transactions
    .select("merchant", "id")
    .intersect(
        df_transactions
        .filter(F.col("txn_type") == "REFUND")
        .select("merchant", "id")
    )
)

kept_rows = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("amount") >= 3400, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.max(F.when(F.col("txn_type") == "REFUND", F.col("amount"))).alias("refund_max_amount"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("txn_time").alias("last_txn_time"),
    )
)
