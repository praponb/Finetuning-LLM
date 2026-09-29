from pyspark.sql import functions as F

kept_keys = (
    df_transactions
    .select("merchant", "id")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_type").isin(["REFUND", "FEE", "CREDIT"]))
        .select("merchant", "id")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("amount") >= 3300, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.max(F.when(F.col("merchant") == "Amazon", F.col("amount"))).alias("amazon_max_amount"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.min("txn_time").alias("first_txn_time"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Amazon", 1)), F.lit(0)), 2).alias("amount_per_amazon"),
        F.max("txn_time").alias("ended_at"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_rows"),
    )
    .orderBy(F.col("amazon_max_amount").desc())
)
