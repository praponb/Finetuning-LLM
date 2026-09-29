from pyspark.sql import functions as F

matched_keys = (
    df_transactions
    .select("merchant", "id")
    .intersect(
        df_transactions
        .filter(F.hour("txn_time").between(8, 13))
        .select("merchant", "id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("amount") >= 3900, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.min("txn_time").alias("started_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max(F.when(F.col("merchant") == "Starbucks", F.col("amount"))).alias("starbucks_max_amount"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Starbucks", 1)), F.lit(0)), 2).alias("amount_per_starbucks"),
    )
    .orderBy(F.col("amount_per_starbucks").desc_nulls_last())
)
