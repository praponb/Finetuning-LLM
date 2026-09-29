from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("t.amount_range"),
        F.col("t.min_amount"),
    )
)
