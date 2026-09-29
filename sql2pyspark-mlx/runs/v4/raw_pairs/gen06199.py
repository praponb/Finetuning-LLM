from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-02-20 15:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        F.col("txn_time").cast("date").alias("txn_time_day"),
        F.initcap(F.trim("merchant")).alias("merchant_title"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.account_type"))
)
