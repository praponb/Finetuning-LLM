from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("account_id").isin([9, 36]))
    .groupBy("account_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .filter(F.col("tba.stddev_amount") > 73)
    .select(F.col("a.account_type"), F.col("a.status"), F.col("tba.stddev_amount"))
    .orderBy(F.col("status").desc())
)
