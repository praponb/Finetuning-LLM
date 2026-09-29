from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        F.upper("status").alias("status_upper"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.hour(F.col("ch.txn_time")).between(6, 11)), "left_anti")
)
