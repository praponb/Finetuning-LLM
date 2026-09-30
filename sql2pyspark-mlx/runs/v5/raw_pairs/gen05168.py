from pyspark.sql import functions as F

matched_keys = (
    df_transactions
    .select("txn_type", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("merchant").isin(["Grab", "Lazada"]))
        .select("txn_type", "merchant")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("quarter", "txn_time").alias("quarter_start"))
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .orderBy(F.col("avg_amount").desc())
)
