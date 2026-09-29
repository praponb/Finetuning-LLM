from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-04-28 09:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.lower("merchant").alias("merchant_lower"),
    )
)

result = (
    base
    .select("id", "merchant")
    .exceptAll(
        df_transactions
        .filter(F.col("account_id").isin([21, 26]))
        .select("id", "merchant")
    )
)
