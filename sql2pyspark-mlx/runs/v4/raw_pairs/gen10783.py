from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.to_date("txn_time").alias("txn_time_date"),
        F.round((F.col("amount") * 0.9), 2).alias("amount_adjusted"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)

result = (
    base
    .select("txn_type", "account_id")
    .subtract(
        df_transactions
        .filter(~F.col("merchant").isin(["Starbucks", "Grab", "7-Eleven"]))
        .select("txn_type", "account_id")
    )
)
