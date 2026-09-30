from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("opened_date").between("2023-04-01", "2023-09-16"))

accounts_stats = (
    filtered_accounts
    .groupBy("status")
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
)

result = (
    accounts_stats
    .filter(F.col("stddev_balance") > 22)
    .select("status", "stddev_balance")
    .orderBy(F.col("stddev_balance").desc())
)
