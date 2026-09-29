from pyspark.sql import functions as F

src = df_accounts.filter(F.col("balance").isNotNull())

accounts_stats = (
    src
    .groupBy("account_type", "status")
    .agg(F.count("balance").alias("balance_count"))
)

result = accounts_stats.select("account_type", "status")
