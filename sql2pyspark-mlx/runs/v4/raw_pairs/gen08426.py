from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-08-25").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.length("txn_type").alias("txn_type_length"),
    )
)

cleaned_transactions = (
    cleaned
    .filter(F.col("txn_time") > F.lit("2023-05-19").cast("date"))
    .select(
        "id",
        "txn_type",
        "is_flagged",
        F.trim("merchant").alias("clean_merchant"),
        F.substring("txn_type", 1, 4).alias("txn_type_prefix"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

cleaned_transactions_2 = (
    cleaned_transactions
    .filter(F.col("txn_type") != "FEE")
    .select(
        "id",
        "txn_type",
        "merchant_clean",
        F.substring("txn_type", 1, 3).alias("txn_type_prefix_2"),
    )
)

result = (
    cleaned_transactions_2
    .select("id", "txn_type", "merchant_clean", "txn_type_prefix_2")
)
