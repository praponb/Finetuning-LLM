from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("txn_time").alias("min_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.amount_range") > 77)
    .select(F.col("a.opened_date"), F.col("as.amount_range"), F.col("as.min_txn_time"))
    .orderBy("opened_date")
)
