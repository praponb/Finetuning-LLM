from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2024-05-27"))) & F.col("merchant").isNotNull() & (F.col("amount") >= 1500))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.date_trunc("WEEK", "txn_time").alias("txn_time_period"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

kept_keys = (
    base
    .select("txn_type", "id")
    .intersect(
        df_transactions
        .filter(F.col("merchant") == "Shell")
        .select("txn_type", "id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.to_date("txn_time") < "2024-11-01")
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_time_period",
        "merchant_filled",
        F.round((F.col("amount") * 1.1), 2).alias("amount_adjusted"),
        F.to_date("txn_time_period").alias("txn_time_period_date"),
    )
)

filtered = (
    cleaned
    .filter(F.col("merchant").isNotNull() & F.hour("txn_time_period").between(9, 13))
    .select("*", F.round("amount_adjusted", 2).alias("amount_adjusted_rounded"))
)

result = (
    filtered
    .groupBy("merchant")
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("amount_adjusted", 0.95).alias("amount_adjusted_p95"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount_adjusted"))).alias("refund_total_amount_adjusted"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_rows"),
    )
    .orderBy(F.col("merchant").desc())
)
