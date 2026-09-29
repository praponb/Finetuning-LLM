from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("amount") >= 2200)
    .groupBy("account_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.stddev_amount") > 56)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("tba.stddev_amount"),
    )
)
