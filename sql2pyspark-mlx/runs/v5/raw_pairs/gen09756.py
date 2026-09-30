from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-09-29 10:00:00").cast("timestamp")) & (F.col("amount") <= 1300))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.substring("txn_type", 1, 2).alias("txn_type_prefix"),
        F.year("txn_time").alias("txn_time_year"),
    )
)

result = (
    cleaned
    .select("account_id", "txn_type")
    .intersectAll(
        df_transactions
        .filter(F.col("amount") >= 3400)
        .select("account_id", "txn_type")
    )
)
