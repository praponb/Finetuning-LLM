from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-11-05"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        F.floor("amount").alias("amount_whole"),
        F.when(F.col("merchant") == "Lazada", 1).otherwise(0).alias("is_lazada"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

result = (
    cleaned
    .select("merchant", "account_id")
    .exceptAll(
        df_transactions
        .filter(F.col("amount").isNotNull())
        .select("merchant", "account_id")
    )
)
