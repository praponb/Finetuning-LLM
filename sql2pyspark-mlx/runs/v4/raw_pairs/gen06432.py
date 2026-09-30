from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .select(
        "*",
        F.substring("txn_type", 1, 3).alias("txn_type_prefix"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

result = (
    base
    .select("txn_type", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("account_id").isin([8, 27, 33, 35]))
        .select("txn_type", "merchant")
    )
)
