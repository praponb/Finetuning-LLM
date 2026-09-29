from pyspark.sql import functions as F

kept_keys = (
    df_transactions
    .select("txn_type")
    .subtract(
        df_transactions
        .filter(F.col("amount").between(1300, 2500))
        .select("txn_type")
    )
)

kept_rows = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(~F.col("merchant").eqNullSafe(F.lit("Amazon")))
    .withColumn("amount_rounded", F.round("amount", 0))
)

matched_keys = (
    base
    .select("txn_type", "merchant")
    .intersect(
        df_transactions
        .filter(~F.col("txn_type").isin(["REFUND", "CREDIT", "DEBIT"]))
        .select("txn_type", "merchant")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

base_transactions = (
    verified
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-09-13 16:00:00")))
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.when(F.col("txn_type").isin(["DEBIT", "FEE"]), "Priority").otherwise("Other").alias("txn_type_group"),
    )
)

result = (
    base_transactions
    .groupBy(
        F.when(F.col("amount") >= 4500, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.avg("amount").alias("avg_amount"),
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
    .orderBy(F.col("num_merchants").desc_nulls_last())
)
