from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("txn_type") != "FEE")
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("UNKNOWN")))
)

result = (
    valid
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .filter(F.col("p2.account_type").isin(["SAVINGS", "CHECKING"]))
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.txn_time",
        "a.merchant",
        "a.is_flagged_flag",
        F.col("p2.account_type"),
    )
)
