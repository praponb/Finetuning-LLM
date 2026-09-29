from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .groupBy(F.date_trunc("MONTH", F.col("accounts.opened_date")).alias("opened_month"))
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("cnt").desc(), "opened_month")
)
