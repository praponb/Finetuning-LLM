from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("txn_time") > F.to_timestamp(F.lit("2024-03-15 00:00:00"))) & (F.col("txn_time") >= "2025-07-01"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0).alias("is_7_eleven"),
    )
)

result = (
    base
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.opened_date"), F.col("b.balance"), F.col("b.account_type"))
)
