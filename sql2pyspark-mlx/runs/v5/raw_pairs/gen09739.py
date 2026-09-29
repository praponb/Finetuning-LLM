from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.cnt") > 6)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.account_type"),
        F.col("at.cnt"),
        F.col("at.refund_count"),
    )
)
