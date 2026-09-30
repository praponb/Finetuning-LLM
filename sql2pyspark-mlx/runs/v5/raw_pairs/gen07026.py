from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-10-09").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.upper("txn_type").alias("txn_type_upper"),
    )
)

result = (
    valid
    .select("txn_type", "id")
    .subtract(
        df_transactions
        .filter(F.col("txn_type") == "DEBIT")
        .select("txn_type", "id")
    )
)
