from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((~F.col("merchant").eqNullSafe(F.lit("Grab"))) & ~F.col("merchant").isin(["Lazada", "Amazon", "Grab"]))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("NONE")))
)

result = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.balance") <= 75000)
    .select(
        "s.id",
        "s.account_id",
        "s.amount",
        "s.txn_time",
        "s.is_flagged_flag",
        "s.merchant_clean",
        F.col("a.balance"),
    )
)
