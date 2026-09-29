from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((~F.col("merchant").eqNullSafe(F.lit("Shell"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
        F.concat("merchant", F.lit(", "), "txn_type").alias("merchant_txn_type"),
    )
)

result = (
    base
    .select("account_id", "id")
    .subtract(
        df_transactions
        .filter(F.col("account_id") == 10)
        .select("account_id", "id")
    )
)
