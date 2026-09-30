from pyspark.sql import functions as F

kept_keys = (
    df_transactions
    .select("txn_type", "merchant")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_time") >= "2025-02-01")
        .select("txn_type", "merchant")
    )
)

kept_rows = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        F.when(F.col("merchant").rlike("^(Lazada|Grab)$"), "CORE").when(F.col("merchant").rlike("^(7-Eleven|Amazon)$"), "PRIORITY").otherwise("GROUP_B").alias("merchant_zone"),
    )
    .orderBy("amount")
)
