from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("account_type") == "LOAN") & F.col("balance").between(57000, 69000) & F.month("opened_date").isin([2, 4, 12]))
)

accounts_stats = (
    base
    .groupBy("account_type")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.avg("balance").alias("avg_balance"),
    )
)

result = accounts_stats.filter(F.col("avg_balance") > 40000)
