from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("account_id") == 11)
    .groupBy("account_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.stddev_amount") > 21)
    .select(F.col("a.balance"), F.col("a.opened_date"), F.col("at.stddev_amount"))
)
