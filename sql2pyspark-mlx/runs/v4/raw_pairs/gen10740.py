from pyspark.sql import functions as F

kept_keys = (
    df_transactions
    .select("id")
    .intersectAll(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("amount") >= 3000, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.max(F.when(F.col("merchant") == "Shell", F.col("amount"))).alias("shell_max_amount"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
    )
    .orderBy(F.col("num_txn_types").desc())
)
