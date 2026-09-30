from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-04-09"))) & (F.col("account_id") == 2))
    .select(
        "id",
        "amount",
        "merchant",
        "is_flagged",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)

remaining_keys = (
    base
    .select("id")
    .subtract(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("amount_filled") <= 57)
    .select(
        "*",
        (F.col("amount") / F.nullif("amount_filled", F.lit(0))).alias("amount_per_amount_filled"),
    )
)

base_transactions = (
    filtered
    .filter(F.col("merchant").isin(["Shell", "Starbucks", "Amazon"]))
    .select(
        "id",
        "amount",
        "merchant",
        "is_flagged",
        "amount_filled",
        "merchant_filled",
        "amount_per_amount_filled",
        F.round("amount_filled", 0).alias("amount_filled_rounded"),
    )
)

cleaned = (
    base_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "amount",
        "merchant",
        "is_flagged",
        "amount_filled",
        "merchant_filled",
        "amount_per_amount_filled",
        "amount_filled_rounded",
        F.round((F.col("amount_per_amount_filled") / F.col("amount")), 2).alias("amount_per_amount_filled_per_amount"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

grouped = (
    cleaned
    .withColumn("amount_per_amount_filled_per_amount_tier", F.when(F.col("amount_per_amount_filled_per_amount") >= 81, "LARGE").otherwise("SMALL"))
    .withColumn("amount_filled_rounded_tier", F.when(F.col("amount_filled_rounded") >= 63, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["amount_per_amount_filled_per_amount_tier", "amount_filled_rounded_tier"], ["amount_per_amount_filled_per_amount_tier"], []],
        "amount_per_amount_filled_per_amount_tier",
        "amount_filled_rounded_tier",
    )
    .agg(
        F.round(F.avg("amount_filled_rounded"), 2).alias("avg_amount_filled_rounded"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_rows"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.percentile_approx("amount_filled", [0.25, 0.5, 0.75]).alias("amount_filled_quartiles"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_rows_2"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    grouped
    .select(
        "amount_filled_rounded_tier",
        "avg_amount_filled_rounded",
        "7_eleven_rows",
        "amount_filled_quartiles",
        "7_eleven_rows_2",
        "grouping_level",
    )
    .orderBy(F.col("amount_filled_rounded_tier").desc_nulls_last())
)
