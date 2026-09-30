from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .filter(F.col("tba.stddev_amount") >= 31)
    .select(
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("tba.stddev_amount"),
        F.col("tba.amount_range"),
    )
)
