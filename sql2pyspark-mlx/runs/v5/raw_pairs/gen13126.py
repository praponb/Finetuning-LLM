from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.to_date("txn_time") >= "2025-10-01"))
    .withColumn("txn_type_merchant_key", F.concat_ws(",", "txn_type", "merchant"))
    .withColumn("merchant_title", F.initcap(F.trim("merchant")))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("OTHER")))
)

result = (
    base
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.customer_id") == 1)
    .select(
        "src.id",
        "src.account_id",
        "src.amount",
        "src.txn_time",
        "src.merchant",
        "src.merchant_clean",
        F.col("a.status"),
        F.col("a.account_type"),
    )
)
