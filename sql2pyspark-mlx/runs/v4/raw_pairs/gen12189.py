from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(F.col("accounts.account_type") == "CHECKING")
    .groupBy(F.col("accounts.status"))
    .agg(F.round(F.stddev(F.col("transactions.amount")), 2).alias("stddev_amount"))
    .orderBy("stddev_amount")
)
